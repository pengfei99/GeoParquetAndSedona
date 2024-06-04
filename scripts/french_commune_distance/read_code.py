def read_list_from_file(filename):
    with open(filename, 'r') as file:
        lines = file.readlines()
        lines = [line.rstrip('\n') for line in lines]
        return lines


def get_code_list():
    parent_dir = "/tmp/code_split_test"
    start = 0
    end = 1

    code_list = []

    for i in range(start, end + 1):
        filename = f"{parent_dir}/part_{i}.txt"
        lines_read = read_list_from_file(filename)
        print(f"file name: {filename}")
        for line in lines_read:
            code = line.strip()
            code_list.append(code)
    return code_list


code_list = get_code_list()
print(len(code_list))
