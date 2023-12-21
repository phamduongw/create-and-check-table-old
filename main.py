import os
import shutil
from utils import read_file_content, read_env_file, create_folder
from stream import create_stream
from connector import create_connector


def main(table_name):
    try:
        raw_index = len(os.listdir("build")) + 1
    except OSError:
        raw_index = 1

    table_index = "{:02d}".format(raw_index)

    env_file_content = (
        read_file_content(".env")
        .replace("{INDEX}", table_index)
        .replace("{TABLE_NAME}", table_name)
        .split("\n")
    )
    read_env_file(env_file_content)

    # 02.STREAM
    status = create_stream(table_name)

    if status:
         # 01.DDL
        create_folder(os.environ["DDL_FOLDER"])
        
        # 03.CONNECTOR
        create_connector(table_name)
        # Excel 
        shutil.copy("template/Checklist-golive-SEAB_SALARY_CUSTOMER.xlsx", os.environ["EXCEL_FILE"])
    

if __name__ == "__main__":
    table_names = read_file_content("data/table_name.txt").split('\n')
    for table_name in table_names:
        if(table_name):
            main(table_name.strip())
