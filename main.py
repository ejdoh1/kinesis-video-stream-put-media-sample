"""
Script to put media to Kinesis Video Stream
"""

import os
from kvs_client import KvsClient, InputParams

if __name__ == "__main__":
    input_params = InputParams(
        stream_name=os.environ["STREAM_NAME"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        video_file_path=os.environ["VIDEO_FILE_PATH"],
    )
    kvs_client = KvsClient(input_params=input_params)
    kvs_client.initialise()
    kvs_client.put_media()
