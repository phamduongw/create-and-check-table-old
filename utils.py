import os
import base64


def read_file_content(path):
    with open(path, "r") as file:
        return file.read()


def create_folder(path):
    if path and not os.path.exists(path):
        os.makedirs(path)


def write_to_file(path, content):
    create_folder("/".join(path.split("/")[:-1]))
    with open(path, "w") as file:
        file.write(content)


def read_env_file(file):
    for line in file:
        if line and not line.startswith("#"):
            key, value = line.strip().split("=", 1)
            os.environ[key] = value


def getBase64Credentials(username, password):
    credentials = "{}:{}".format(username, password)
    credentials_bytes = credentials.encode("utf-8")
    base64_credentials = base64.b64encode(credentials_bytes).decode("utf-8")
    return base64_credentials
