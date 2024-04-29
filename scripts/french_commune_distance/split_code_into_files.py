import sys

from pyspark.sql import SparkSession


def split_list_into_parts(list_to_split, num_parts):
    # Calculate the size of each part
    part_size = len(list_to_split) // num_parts

    # Split the list into parts
    parts = [list_to_split[i * part_size:(i + 1) * part_size] for i in range(num_parts)]

    # Handle the case where the list cannot be evenly divided
    if len(list_to_split) % num_parts != 0:
        remaining = list_to_split[num_parts * part_size:]
        for i, item in enumerate(remaining):
            parts[i].append(item)

    return parts


# Function to write a list to a file
def write_list_to_file(filename, lines):
    with open(filename, 'w') as file:
        for line in lines:
            file.write(line + '\n')


def main():
    # get argument from command line
    if len(sys.argv) != 2:
        print("Usage: python split_code_column.py <partition_number>")
        return

    # set osrm host
    part_num = int(sys.argv[1])

    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Example PySpark Job") \
        .getOrCreate()

    # converted centroid parquet file path
    fr_zone_file_path = "/home/pliu/data/converted_centroid_of_french_commune"
    converted_centroid_df = spark.read.parquet(fr_zone_file_path)
    converted_centroid_df.cache()
    converted_centroid_df.show(5)

    # input argument
    full_code_list = [row.insee for row in converted_centroid_df.select("insee").collect()]
    parent_dir = "/tmp/code_split_test"

    # Split the list into 16 parts
    parts = split_list_into_parts(full_code_list, part_num)

    # Write each part to a separate file
    for i, part in enumerate(parts):
        filename = f"{parent_dir}/part_{i}.txt"
        write_list_to_file(filename, part)
        print(f"Part {i} has been written to {filename}.")

    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
