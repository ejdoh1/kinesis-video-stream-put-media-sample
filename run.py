import os
from kvs_client import KvsClient, InputParams

if __name__ == "__main__":
    input_params = InputParams(
        stream_name=os.environ["STREAM_NAME"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    kvs_client = KvsClient(input_params=input_params)
    kvs_client.initialise()
    kvs_client.put_media()
