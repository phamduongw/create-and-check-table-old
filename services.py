import os
import json

from utils import getBase64Credentials


def list_streams_extended():
    command = """curl '{}/ksql' \
    -H 'Content-Type: application/json' \
    -H 'Authorization: Basic {}' \
    -d '{{
        "ksql": "LIST STREAMS EXTENDED;"
    }}'""".format(
        os.environ.get("KSQLDB_URL"),
        getBase64Credentials(
            os.environ.get("KSQLDB_USERNAME"), os.environ.get("KSQLDB_PASSWORD")
        ),
    )
    response = os.popen(command).read()
    return json.loads(response)


def get_schema_by_table_name(table_name):
    command = "curl '{}/subjects/T24SS_{}-value/versions/latest'".format(
        os.environ["CONTROL_CENTER_URL"], table_name
    )
    response = os.popen(command).read()
    return json.loads(response)["schema"]
