# AWS Kinesis Video Stream PutMedia Python Lambda Sample

Upload a video to AWS Kinesis Video Stream using the PutMedia API from a Python Lambda function.

## Prerequisites

- Python3
- AWS Account
- Serverless Framework

## How to run

```sh
npm install

export STREAM_NAME="REPLACE_ME" # will be created if it doesn't exist
export AWS_PROFILE="REPLACE_ME" # optional
export VIDEO_FILE_PATH="Big_Buck_Bunny_1080_10s_1MB.mkv"

sls deploy

# upload the video using the API Gateway endpoint URL from the output of the previous command
# curl https://xxxxx.execute-api.ap-southeast-2.amazonaws.com/dev/upload

```

## Encode the sample video

If you use the .webm, you will get an error like this in the AWS Kinesis Video Stream console media playback:

```
Missing codec private data error
The fragment did not contain any codec private data. Ensure that the producer is generating valid codec private data.
```

So you need to encode it with ffmpeg:

`ffmpeg -i Big_Buck_Bunny_1080_10s_1MB.webm -c:v libx264 -preset veryslow -crf 40 -c:a copy Big_Buck_Bunny_1080_10s_1MB.mkv`

## What it should look like

![screenshot](./screenshot.png)

```sh
(venv) kinesis-video-stream-put-media-sample $ curl https://xxxxx.execute-api.ap-southeast-2.amazonaws.com/dev/upload
[
    {
        "FragmentLengthInMilliseconds": 4933,
        "FragmentNumber": "91343852333182194963746262452351523100320850344",
        "FragmentSizeInBytes": 369203,
        "ProducerTimestamp": "2023-11-08 13:47:45.470000+00:00",
        "ServerTimestamp": "2023-11-08 13:47:45.527000+00:00"
    },
    {
        "FragmentLengthInMilliseconds": 1634,
        "FragmentNumber": "91343852333182194973649782766634565303615586077",
        "FragmentSizeInBytes": 143468,
        "ProducerTimestamp": "2023-11-08 13:47:53.803000+00:00",
        "ServerTimestamp": "2023-11-08 13:47:45.542000+00:00"
    },
    {
        "FragmentLengthInMilliseconds": 3233,
        "FragmentNumber": "91343852333182194968698022609493044201660210810",
        "FragmentSizeInBytes": 218775,
        "ProducerTimestamp": "2023-11-08 13:47:50.537000+00:00",
        "ServerTimestamp": "2023-11-08 13:47:45.534000+00:00"
    }
]
```

## Links

- [Amazon AWS Kinesis Video Boto GetMedia/PutMedia](https://stackoverflow.com/a/59551573)
- [H.264 Video Encoding Guide](https://trac.ffmpeg.org/wiki/Encode/H.264)  
