import os
from utils import read_file_content, write_to_file, read_env_file
from stream import create_stream


def main():
    try:
        raw_index = len(os.listdir("build")) + 1
    except OSError:
        raw_index = 1

    table_index = "{:02d}".format(raw_index)
    table_name = read_file_content("data/table_name.txt").strip()

    env_file_content = (
        read_file_content(".env")
        .replace("{INDEX}", table_index)
        .replace("{TABLE_NAME}", table_name)
        .split("\n")
    )
    read_env_file(env_file_content)

    create_stream(table_name)


if __name__ == "__main__":
    main()
