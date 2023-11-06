"""
Script to put media to Kinesis Video Stream
"""

import os
import time


from kvs_client import InputParams, KvsClient

if __name__ == "__main__":
    input_params = InputParams(
        stream_name=os.environ["STREAM_NAME"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        video_file_path=os.environ["VIDEO_FILE_PATH"],
    )
    kvs_client = KvsClient(input_params=input_params)
    kvs_client.initialise()

    time_pre_put_media = time.time()
    result = kvs_client.put_media(chunk_size=100000)
    time_post_put_media = time.time()

    # get the first fragment number
    fragment_number = result[0]["FragmentNumber"]
    print(f"Fragment number: {fragment_number}")

    # get the media
    media = kvs_client.get_media(
        fragment_number=fragment_number, output_file_path="output.mkv"
    )

    # get the list of fragments
    fragments = kvs_client.list_fragments(
        stream_name=input_params.stream_name,
        start_timestamp=time_pre_put_media,
        end_timestamp=time_post_put_media,
    )
    print(f"Fragment count: {len(fragments)}")
    print(f"Fragment list: {fragments}")
