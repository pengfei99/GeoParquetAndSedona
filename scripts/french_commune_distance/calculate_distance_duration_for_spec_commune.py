import os
import sys
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
import requests
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col, split

ENV_KEY = "OSRM_HOST"


def read_code_list_from_file(filepath: str):
    """
    This function read the insee code list part file and return a list of insee code (str)
    :param filepath: path for the part file
    :type filepath: str
    :return: List[str]
    :rtype:
    """
    with open(filepath, 'r') as file:
        lines = file.readlines()
        # must remove the '\n'
        lines = [line.rstrip('\n') for line in lines]
        return lines


# required functions
def get_osrm_host(env_key, default: str = "127.0.0.1:5000") -> str:
    """
    This function read the env var "OSRM_HOST", and return the value. If does not exist return a default value
    :param env_key:
    :type env_key:
    :param default:
    :type default:
    :return:
    :rtype:
    """
    return os.getenv(env_key, default)


def get_route(lat_start: str, long_start: str, lat_end: str, long_end: str,
              show_steps: str = "false") -> dict:
    """
    This function calls the orsm rest api to get possible routes in car drive mode
    :param lat_start: latitude of the starting point
    :param long_start: longitude of the starting point
    :param lat_end: latitude of the ending point
    :param long_end: longitude of the ending point
    :param show_steps: flag to indicate whether to show steps of the routes or not
    :return:
    """
    host = get_osrm_host(ENV_KEY)
    start_point = f"{long_start},{lat_start}"
    end_point = f"{long_end},{lat_end}"
    # Define the URL
    url = f"http://{host}/route/v1/driving/{start_point};{end_point}?steps={show_steps}"

    # Make the GET request
    response = requests.get(url, verify=False, timeout=10)
    json_response = None
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Print the response content
        json_response = response.json()
    else:
        print("Error:", response.status_code)
    return json_response


def parse_route_json(input_route: dict) -> Tuple[float, float]:
    """
    This function parse the route result json that is returned by the orsm api.
    :param input_route:
    :return: a tuple (distance, duration)
    """
    try:
        route = input_route.get('routes', [])[0]
        if not route:
            return 0.0, 0.0

        distance = float(route.get("distance", 0))
        duration = round(float(route.get("duration", 0)) / 60, 2)
        return distance, duration

    except (IndexError, TypeError, ValueError, KeyError):
        return 0.0, 0.0


def calculate_distance_duration(lat_start: str, long_start: str, lat_end: str, long_end: str) -> (float, float):
    route = get_route(lat_start, long_start, lat_end, long_end)
    return parse_route_json(route)


def calculate_distance_duration_str(lat_start: str, long_start: str, lat_end: str, long_end: str) -> str:
    distance, duration = calculate_distance_duration(lat_start, long_start, lat_end, long_end)
    return f"{distance};{duration}"


@udf(returnType=StringType())
def get_distance_duration(lat_start: str, long_start: str, lat_end: str, long_end: str):
    return calculate_distance_duration_str(lat_start, long_start, lat_end, long_end)


def calculate_distance_duration_matrix_in_patch(route_matrix_df: DataFrame, output_file_path: str):
    """
    This function calculate the distance and duration between src and dest commune in the giving matrix
    :param route_matrix_df:
    :param output_file_path:
    :param patch_size:
    :param partition_num:
    :return:
    """
    # 3. calculate the distance and duration
    distance_duration_df = route_matrix_df.withColumn("distance_duration",
                                                        get_distance_duration(col("source_lat"), col("source_long"),
                                                                              col("dest_lat"),
                                                                              col("dest_long"))).select(
        "source_nom", "source_insee", "dest_nom", "dest_insee", "distance_duration").withColumn("distance(meter)",
                                                                                                split(
                                                                                                    col("distance_duration"),
                                                                                                    ";")[
                                                                                                    0]).withColumn(
        "duration(minutes)", split(col("distance_duration"), ";")[1]).drop("distance_duration")
    # 4. write the result into a parquet file
    distance_duration_df.write.mode("append").partitionBy("source_insee").parquet(output_file_path)


def main():
    # get argument from command line
    if len(sys.argv) != 3:
        print("Usage: python calculate_distance_duration.py <osrm_host>")
        return

    # step1: set osrm host
    osrm_host = str(sys.argv[1])
    os.environ[ENV_KEY] = osrm_host

    # step2: Create a SparkSession
    spark = SparkSession.builder \
        .appName("Extra route duration calculation") \
        .getOrCreate()

    # step3: read the converted french commune centroid parquet file
    fr_zone_file_path = "/home/pliu/data/converted_centroid_of_french_commune"
    converted_centroid_df = spark.read.parquet(fr_zone_file_path)
    converted_centroid_df.cache()
    converted_centroid_df.show(5)

    # step4: for each code list split part, calculate the distance and duration matrix
    for i in range(start_part, end_part+1):
        filename = f"{code_list_parent_dir}/part_{i}.txt"

        code_list = read_code_list_from_file(filename)
        print(f"code list: {code_list}")
        calculate_distance_duration_matrix_in_patch(code_list, converted_centroid_df,
                                                    output_file_path,
                                                    partition_num=partition)

    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
