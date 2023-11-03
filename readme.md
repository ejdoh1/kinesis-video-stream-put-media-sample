# AWS Kinesis Video Stream PutMedia Python Sample

## Prerequisites

- Python3
- ffmpeg
- AWS Account

## How to run

```sh
export AWS_ACCESS_KEY_ID="REPLACE_ME"
export AWS_SECRET_ACCESS_KEY="REPLACE_ME"
export STREAM_NAME="REPLACE_ME" # will be created if it doesn't exist
export AWS_DEFAULT_REGION="ap-southeast-2"
export VIDEO_FILEPATH="./blue.mkv"

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python3 main.py
```

## How to generate a sample video

`ffmpeg -f lavfi -i color=green:s=1280x720 -vframes 1 green.mkv`

## Links

- [Amazon AWS Kinesis Video Boto GetMedia/PutMedia](https://stackoverflow.com/a/59551573)
