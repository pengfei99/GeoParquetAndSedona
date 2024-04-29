def read_list_from_file(filename):
    with open(filename, 'r') as file:
        lines = file.readlines()
        lines = [line.rstrip('\n') for line in lines]
        return lines


parent_dir = "/tmp/code_split_test"
start = 0
end = 0

for i in range(start, end+1):
    filename = f"{parent_dir}/part_{i}.txt"
    lines_read = read_list_from_file(filename)
    print(f"file name: {filename}")
    print("Contents of the file:")
    for line in lines_read:
        code = line.strip()
        print(f"type: {type(code)}, value: {code}")

