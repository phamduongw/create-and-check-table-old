import os
import re
import json
from ksqldb_services import list_streams_extended
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
    write_to_file(
        os.environ["CDC_STREAM_1_PATH"],
        read_file_content("template/cdc_1.txt").replace(
            "{TABLE_NAME}", os.environ["TABLE_NAME"]
        ),
    )
    write_to_file(
        os.environ["INIT_STREAM_1_PATH"],
        read_file_content("template/init_1.txt").replace(
            "{TABLE_NAME}", os.environ["TABLE_NAME"]
        ),
    )


# Stream 2
def create_statement_of_stream_2():
    write_to_file(
        os.environ["CDC_STREAM_2_PATH"],
        read_file_content("template/cdc_2.txt").replace(
            "{TABLE_NAME}", os.environ["TABLE_NAME"]
        ),
    )
    write_to_file(
        os.environ["INIT_STREAM_2_PATH"],
        read_file_content("template/init_2.txt").replace(
            "{TABLE_NAME}", os.environ["TABLE_NAME"]
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
                field_name.strip() + check_field_name_in_schema(field_name, SCHEMA)
            )
    return field_names


def get_statement_of_stream_3(stream_name):
    for stream_info in ALL_STREAMS_AND_TOPICS:
        if stream_info["name"] == stream_name:
            field_names_raw = get_field_names_of_statement(stream_info["statement"])

            field_names = "\n".join(
                "\t" + field_name.decode("utf-8") for field_name in field_names_raw
            )

            write_to_file(
                os.environ["CDC_STREAM_3_PATH"],
                read_file_content("template/cdc_3.txt")
                .replace("{TABLE_NAME}", os.environ["TABLE_NAME"])
                .replace("{FIELD_NAMES}", field_names.encode("utf-8")),
            )
            write_to_file(
                os.environ["INIT_STREAM_3_PATH"],
                read_file_content("template/init_3.txt")
                .replace("{TABLE_NAME}", os.environ["TABLE_NAME"])
                .replace("{FIELD_NAMES}", field_names.encode("utf-8")),
            )


def create_statement_of_stream_3(ods_stream):
    # ALL_STREAMS_AND_TOPICS
    global ALL_STREAMS_AND_TOPICS
    ALL_STREAMS_AND_TOPICS = get_all_streams_and_topics()

    # SCHEMA
    global SCHEMA
    with open("data/schema.json", "r") as file:
        SCHEMA = [field["name"] for field in json.load(file)["fields"]]

    # REGEX PATTERN
    global PATTERN
    PATTERN = re.compile(r".+XMLRECORD\['(.+)'\].*\s(\w+),?")

    stream_flow = get_stream_flow("ODS_{}".format(ods_stream.strip()))[::-1]

    for stream_name in stream_flow:
        if len(stream_flow) == 1:
            print("-- {}\n{}\n".format(stream_name, "ERROR!"))
            break
        if "ETL" in stream_name:
            get_statement_of_stream_3(stream_name)
            break
        if "ODS" in stream_name:
            get_statement_of_stream_3(stream_name)
            break


def create_stream(ods_stream):
    create_statement_of_stream_1()
    create_statement_of_stream_2()
    create_statement_of_stream_3(ods_stream)
