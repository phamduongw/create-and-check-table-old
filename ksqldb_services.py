import os
import json

from utils import read_env_file, getBase64Credentials

read_env_file()

KSQLDB_URL = os.environ.get("KSQLDB_URL")
BASE64_CREDENTIALS = getBase64Credentials(
    os.environ.get("KSQLDB_USERNAME"), os.environ.get("KSQLDB_PASSWORD")
)


def list_streams_extended():
    command = """curl \
    --location '{}/ksql' \
    --header 'Content-Type: application/json' \
    --header 'Authorization: Basic {}' \
    --data '{{
        "ksql": "LIST STREAMS EXTENDED;"
    }}'""".format(
        KSQLDB_URL, BASE64_CREDENTIALS
    )

    response = os.popen(command).read()

    return json.loads(response)
