"""
Script to put media to Kinesis Video Stream
"""

import json
import os
import time

from kvs_client import KvsClient


def handler(_event, _context):
    """
    AWS Lambda handler
    """

    # get environment variables
    stream_name = os.environ["STREAM_NAME"]
    video_file_path = os.environ["VIDEO_FILE_PATH"]

    # initialise Kinesis Video Stream client
    kvs_client = KvsClient(
        stream_name=stream_name,
        video_file_path=video_file_path,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=os.environ["AWS_SESSION_TOKEN"],
    )
    kvs_client.initialise()

    # put media to Kinesis Video Stream
    time_pre_put_media = time.time()
    kvs_client.put_media(chunk_size=100000)
    time_post_put_media = time.time()

    # get the list of fragments
    fragments = kvs_client.list_fragments(
        stream_name=stream_name,
        start_timestamp=time_pre_put_media,
        end_timestamp=time_post_put_media,
    )

    # return the list of fragments
    return {
        "statusCode": 200,
        "body": json.dumps(
            fragments,
            indent=4,
            sort_keys=True,
            default=str,
        ),
    }
