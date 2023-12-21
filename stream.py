import os
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


def get_statement_of_stream_3(is_exist, stream_name):
    if is_exist:
        for stream_info in ALL_STREAMS_AND_TOPICS:
            if stream_info["name"] == stream_name:
                write_to_file(os.environ["CDC_STREAM_3_PATH"], stream_info["statement"])
                write_to_file(
                    os.environ["INIT_STREAM_3_PATH"], stream_info["statement"]
                )
    else:
        print("-- {}\n{}\n\n".format(stream_name, "ERROR!"))


def create_statement_of_stream_3(ods_stream):
    global ALL_STREAMS_AND_TOPICS
    ALL_STREAMS_AND_TOPICS = get_all_streams_and_topics()
    stream_flow = get_stream_flow("ODS_{}".format(ods_stream.strip()))[::-1]
    for stream_name in stream_flow:
        if len(stream_flow) == 1:
            get_statement_of_stream_3(False, stream_name)
            break

        if "ETL" in stream_name:
            get_statement_of_stream_3(True, stream_name)
            break

        if "ODS" in stream_name:
            get_statement_of_stream_3(True, stream_name)
            break

    field_name = []
    cdc_3 = read_file_content(os.environ["CDC_STREAM_3_PATH"]).split("\n")
    for line in cdc_3:
        if "DATA.XMLRECORD" in line:
            field_name.append(line.strip())


def create_stream(ods_stream):
    create_statement_of_stream_1()
    create_statement_of_stream_2()
    create_statement_of_stream_3(ods_stream)
