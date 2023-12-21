import os
import json

from utils import getBase64Credentials


def list_streams_extended():
    command = """curl \
    --location '{}/ksql' \
    --header 'Content-Type: application/json' \
    --header 'Authorization: Basic {}' \
    --data '{{
        "ksql": "LIST STREAMS EXTENDED;"
    }}'""".format(
        os.environ.get("KSQLDB_URL"),
        getBase64Credentials(
            os.environ.get("KSQLDB_USERNAME"), os.environ.get("KSQLDB_PASSWORD")
        ),
    )
    response = os.popen(command).read()
    return json.loads(response)
