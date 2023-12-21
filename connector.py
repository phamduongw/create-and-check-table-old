import os
from utils import read_file_content, write_to_file


def convert_table_name(table_name):
    template = ""

    for word in table_name.replace("ODS_", "").split("_"):
        template += word.capitalize()

    return "{}".format(template), "{}".format(template)


def create_connector(table_name):
    CDC_FILE = "connector_T_{CONNECTOR_NAME}2Ods.json"
    INIT_FILE = "connector_T_{CONNECTOR_NAME}Init2Ods.json"
    TABLE_NAME = os.environ["TABLE_NAME"]

    cdc_connector_name, init_connector_name = convert_table_name(table_name)

    write_to_file(
        "{}/{}".format(os.environ["CONNECTOR_FOLDER"], INIT_FILE).replace(
            "{CONNECTOR_NAME}", cdc_connector_name
        ),
        read_file_content("template/connector/{}".format(INIT_FILE)).replace(
            "{TABLE_NAME}", TABLE_NAME
        ).replace("{CONNECTOR_NAME}", init_connector_name),
    )
    write_to_file(
        "{}/{}".format(os.environ["CONNECTOR_FOLDER"], CDC_FILE).replace(
            "{CONNECTOR_NAME}", cdc_connector_name
        ),
        read_file_content("template/connector/{}".format(CDC_FILE)).replace(
            "{TABLE_NAME}", TABLE_NAME
        ).replace("{CONNECTOR_NAME}", cdc_connector_name),
    )