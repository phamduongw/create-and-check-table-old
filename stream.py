import os
import re
import json
from services import list_streams_extended, get_schema_by_table_name
from utils import write_to_file, read_file_content


def get_all_streams_and_topics():
    all_streams_and_topics = []

    source_descriptions = list_streams_extended()[0]["sourceDescriptions"]

    for source_description in source_descriptions:
        sinks = []

        for read_query in source_description["readQueries"]:
            for sink in read_query["sinks"]:
                sinks.append(sink)

        all_streams_and_topics.append(
            {
                "name": source_description["name"],
                "sinks": sinks,
                "statement": source_description["statement"],
            }
        )

    return all_streams_and_topics


def get_stream_flow(ods_stream):
    stream_flow = []

    def get_stream_flow_item(ods_stream):
        stream_flow.append(ods_stream)

        for stream_info in ALL_STREAMS_AND_TOPICS:
            for sink in stream_info["sinks"]:
                if sink == ods_stream:
                    get_stream_flow_item(stream_info["name"])

    get_stream_flow_item(ods_stream)

    return stream_flow


# Stream 1
def create_statement_of_stream_1():
    CDC_FILE = "0.1.FBNK_{TABLE_NAME}_v0.1.sql"
    INIT_FILE = "0.1.INIT_FBNK_{TABLE_NAME}_v0.1.sql"

    write_to_file(
        "{}/{}".format(os.environ["INIT_STREAM_FOLDER"], INIT_FILE).replace(
            "{TABLE_NAME}", TABLE_NAME
        ),
        read_file_content("template/stream/{}".format(INIT_FILE)).replace(
            "{TABLE_NAME}", TABLE_NAME
        ),
    )
    write_to_file(
        "{}/{}".format(os.environ["CDC_STREAM_FOLDER"], CDC_FILE).replace(
            "{TABLE_NAME}", TABLE_NAME
        ),
        read_file_content("template/stream/{}".format(CDC_FILE)).replace(
            "{TABLE_NAME}", TABLE_NAME
        ),
    )


# Stream 2
def create_statement_of_stream_2():
    CDC_FILE = "0.2.FBNK_{TABLE_NAME}_MAPPED_v0.1.sql"
    INIT_FILE = "0.2.INIT_FBNK_{TABLE_NAME}_MAPPED_v0.1.sql"

    write_to_file(
        "{}/{}".format(os.environ["INIT_STREAM_FOLDER"], INIT_FILE).replace(
            "{TABLE_NAME}", TABLE_NAME
        ),
        read_file_content("template/stream/{}".format(INIT_FILE)).replace(
            "{TABLE_NAME}", TABLE_NAME
        ),
    )
    write_to_file(
        "{}/{}".format(os.environ["CDC_STREAM_FOLDER"], CDC_FILE).replace(
            "{TABLE_NAME}", TABLE_NAME
        ),
        read_file_content("template/stream/{}".format(CDC_FILE)).replace(
            "{TABLE_NAME}", TABLE_NAME
        ),
    )


# Stream 3
def check_field_name_in_schema(field_name, schema):
    match = PATTERN.match(field_name)
    if match:
        group1, group2 = match.group(1), match.group(2)

        return " -- XMLRECORD['{0}'] = {1} -- FIELD['{2}'] = {3}".format(
            group1, group1 in schema, group2, group2 in schema
        )
    else:
        print("Match failed.\n")


def get_field_names_of_statement(statement):
    field_names = []
    for field_name in statement.split("\n"):
        if "DATA.XMLRECORD" in field_name:
            field_names.append(
                # Except schema
                # field_name.strip() + check_field_name_in_schema(field_name, SCHEMA)
                field_name.strip()
            )
    return field_names


def get_statement_of_stream_3(stream_name):
    CDC_FILE = "0.3.ODS_{TABLE_NAME}_v0.1.sql"
    INIT_FILE = "0.3.INIT_ODS_{TABLE_NAME}_v0.1.sql"

    for stream_info in ALL_STREAMS_AND_TOPICS:
        if stream_info["name"] == stream_name:
            field_names_raw = get_field_names_of_statement(stream_info["statement"])

            field_names = "\n".join(
                "\t" + field_name.decode("utf-8") for field_name in field_names_raw
            )

            write_to_file(
                "{}/{}".format(os.environ["INIT_STREAM_FOLDER"], INIT_FILE).replace(
                    "{TABLE_NAME}", TABLE_NAME
                ),
                read_file_content("template/stream/{}".format(INIT_FILE))
                .replace("{TABLE_NAME}", os.environ["TABLE_NAME"])
                .replace("{FIELD_NAMES}", field_names.encode("utf-8")),
            )
            write_to_file(
                "{}/{}".format(os.environ["CDC_STREAM_FOLDER"], CDC_FILE).replace(
                    "{TABLE_NAME}", TABLE_NAME
                ),
                read_file_content("template/stream/{}".format(CDC_FILE))
                .replace("{TABLE_NAME}", os.environ["TABLE_NAME"])
                .replace("{FIELD_NAMES}", field_names.encode("utf-8")),
            )


def create_statement_of_stream_3(ods_stream):
    stream_flow = get_stream_flow("ODS_{}".format(ods_stream.strip()))[::-1]

    for stream_name in stream_flow:
        if len(stream_flow) == 1:
            print("-- {}\n{}\n".format(stream_name, "ERROR!"))
            return False
        if "ETL" in stream_name:
            get_statement_of_stream_3(stream_name)
            return True
        if "ODS" in stream_name:
            get_statement_of_stream_3(stream_name)
            return True


def create_stream(ods_stream):
    # ALL_STREAMS_AND_TOPICS
    global ALL_STREAMS_AND_TOPICS
    ALL_STREAMS_AND_TOPICS = get_all_streams_and_topics()

    # TABLE NAME
    global TABLE_NAME
    TABLE_NAME = os.environ["TABLE_NAME"]

    # SCHEMA
    global SCHEMA
    with open("data/schema.json", "r") as file:
        SCHEMA = [field["name"] for field in json.load(file)["fields"]]

    # REGEX PATTERN
    global PATTERN
    PATTERN = re.compile(r".+XMLRECORD\['(.+)'\].*\s(\w+),?")

    # Get schema by table name
    # write_to_file("data/schema.json", get_schema_by_table_name(TABLE_NAME))

    
    status = create_statement_of_stream_3(ods_stream)
    if status:
        create_statement_of_stream_1()
        create_statement_of_stream_2()
        return True
    else:
        return False
    
    
