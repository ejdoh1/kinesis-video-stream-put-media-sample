"""
KvsClient is a client for the Kinesis Video Streams service.
"""
import datetime
import hashlib
import hmac
import json
import logging
import time

import boto3
import requests
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)


class InputParams(BaseModel):
    """
    InputParams is the input parameters for the KvsClient.
    """

    aws_access_key_id: str = Field(min_length=1)
    aws_secret_access_key: str = Field(min_length=1)
    aws_default_region: str = Field(default="ap-southeast-2")
    stream_name: str = Field(min_length=1)
    video_file_path: str = Field(default="blue.mkv", min_length=1)


class ChunkGenerator:
    """
    ChunkGenerator is a generator for the request parameters.
    """

    def __init__(self, file_path: str, chunk_size: int = 16000):
        self._chunk_size = chunk_size
        self._data = open(file_path, "rb").read()
        self._pointer = 0
        self._size = len(self._data)

    def __iter__(self):
        return self

    def __next__(self):
        if self._pointer >= self._size:
            raise StopIteration
        chunk_size = min(self._chunk_size, self._size - self._pointer)
        result = self._data[self._pointer : self._pointer + chunk_size]
        self._pointer += chunk_size
        print("Data: chunk size %d" % chunk_size)
        return result


class KvsClient:
    """
    KvsClient is a client for the Kinesis Video Streams service.
    """

    _kinesis_video_service_name = "kinesisvideo"
    _data_endpoint = None  # type: str
    _data_endpoint_host = None  # type: str
    _data_endpoint_region = None  # type: str
    _put_media_endpoint = None  # type: str

    def __init__(self, input_params: InputParams):
        self.input_params = input_params
        self._log = logging.getLogger(__name__)

    def initialise(self):
        """
        configure configures the Kinesis Video Streams client.
        """
        try:
            logging.debug("Creating Kinesis Video Stream.")
            self._create_kinesis_video_stream(stream_name=self.input_params.stream_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceInUseException":
                self._log.info("Stream already exists.")
            else:
                raise e
        self._log.debug("Getting data endpoint.")
        self._data_endpoint = self._get_data_endpoint(
            stream_name=self.input_params.stream_name
        )
        self._log.info("Data endpoint: %s", self._data_endpoint)
        self._data_endpoint_host = self.get_host_from_endpoint(self._data_endpoint)
        self._log.info("Data endpoint host: %s", self._data_endpoint_host)
        self._data_endpoint_region = self._get_region_from_endpoint(self._data_endpoint)
        self._log.info("Data endpoint region: %s", self._data_endpoint_region)

        self._put_media_endpoint = self._data_endpoint + "/putMedia"

    @property
    def data_endpoint(self):
        """
        data_endpoint returns the endpoint for the Kinesis Video Streams service.
        """
        return self._data_endpoint

    def _create_kinesis_video_stream(
        self, stream_name: str, data_retention_in_hours: int = 24
    ):
        """
        create_kinesis_video_stream creates a Kinesis Video Stream.
        """
        return boto3.client("kinesisvideo").create_stream(
            StreamName=stream_name,
            DataRetentionInHours=data_retention_in_hours,
        )

    def _get_data_endpoint(
        self,
        stream_name: str,
        api_name: str = "PUT_MEDIA",
    ) -> str:
        """
        get_endpoint_boto returns the endpoint for the Kinesis Video Streams service.
        Sample https://s-ca658586.kinesisvideo.ap-southeast-2.amazonaws.com
        """
        return boto3.client("kinesisvideo").get_data_endpoint(
            StreamName=stream_name, APIName=api_name
        )["DataEndpoint"]

    def sign(self, key, msg) -> bytes:
        """
        sign returns a signed message.
        """
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    def get_signature_key(
        self, key: str, date_stamp: str, region_name: str, service_name: str
    ) -> bytes:
        """
        get_signature_key returns a derived signing key.
        """
        key_date = self.sign(("AWS4" + key).encode("utf-8"), date_stamp)
        key_region = self.sign(key_date, region_name)
        key_service = self.sign(key_region, service_name)
        return self.sign(key_service, "aws4_request")

    @staticmethod
    def get_host_from_endpoint(endpoint: str) -> str:
        """
        get_host_from_endpoint returns the host from the endpoint.
        Sample https://s-ca658586.kinesisvideo.ap-southeast-2.amazonaws.com
        """
        if not endpoint.startswith("https://"):
            raise ValueError("Endpoint must start with https://")
        return endpoint[len("https://") :]

    @staticmethod
    def _get_region_from_endpoint(endpoint):
        """
        get_region_from_endpoint returns the region from the endpoint.
        """
        if not endpoint.startswith("https://"):
            raise ValueError("Endpoint must start with https://")
        return endpoint[len("https://") :].split(".")[2]

    def get_media(self, fragment_number: str, output_file_path: str = "output.mkv"):
        """
        get_media returns the fragments for the stream.
        """
        media = boto3.client(
            "kinesis-video-media",
            endpoint_url=self._get_data_endpoint(
                stream_name=self.input_params.stream_name,
                api_name="GET_MEDIA",
            ),
        ).get_media(
            StreamName=self.input_params.stream_name,
            StartSelector={
                "StartSelectorType": "FRAGMENT_NUMBER",
                "AfterFragmentNumber": fragment_number,
            },
        )
        with open(output_file_path, "wb") as f:
            f.write(media["Payload"].read())
        return media

    def list_fragments(
        self,
        stream_name: str,
        start_timestamp: float,
        end_timestamp: float,
        api_name: str = "LIST_FRAGMENTS",
        fragment_selector_type: str = "SERVER_TIMESTAMP",  # SERVER_TIMESTAMP | PRODUCER_TIMESTAMP
    ) -> list:
        """
        get_fragments returns the fragments for the stream.
        """
        url = self._get_data_endpoint(
            stream_name=self.input_params.stream_name,
            api_name=api_name,
        )
        return boto3.client(
            "kinesis-video-archived-media",
            endpoint_url=url,
        ).list_fragments(
            StreamName=stream_name,
            FragmentSelector={
                "FragmentSelectorType": fragment_selector_type,
                "TimestampRange": {
                    "StartTimestamp": start_timestamp,
                    "EndTimestamp": end_timestamp,
                },
            },
        )[
            "Fragments"
        ]

    def _generate_headers(self) -> dict:
        """
        generate_headers returns the headers for the request.
        """
        # Create a date for headers and the credential string
        t = datetime.datetime.utcnow()
        start_timestamp = repr(time.time())

        amz_date = t.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = t.strftime("%Y%m%d")  # Date w/o time, used in credential scope
        canonical_uri = "/putMedia"
        canonical_querystring = ""

        canonical_headers = (
            "\n".join(
                [
                    "connection:keep-alive",
                    "content-type:application/json",
                    f"host:{self._data_endpoint_host}",
                    "transfer-encoding:chunked",
                    "user-agent:AWS-SDK-KVS/2.0.2 GCC/7.4.0 Linux/4.15.0-46-generic x86_64",
                    f"x-amz-date:{amz_date}",
                    "x-amzn-fragment-acknowledgment-required:1",
                    "x-amzn-fragment-timecode-type:ABSOLUTE",
                    f"x-amzn-producer-start-timestamp:{start_timestamp}",
                    f"x-amzn-stream-name:{self.input_params.stream_name}",
                ]
            )
            + "\n"
        )

        signed_headers = ";".join(
            [
                "connection",
                "content-type",
                "host",
                "transfer-encoding",
                "user-agent",
                "x-amz-date",
                "x-amzn-fragment-acknowledgment-required",
                "x-amzn-fragment-timecode-type",
                "x-amzn-producer-start-timestamp",
                "x-amzn-stream-name",
            ]
        )

        canonical_request = "\n".join(
            [
                "POST",
                canonical_uri,
                canonical_querystring,
                canonical_headers,
                signed_headers,
                hashlib.sha256("".encode("utf-8")).hexdigest(),
            ]
        )

        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = "/".join(
            [
                date_stamp,
                self._data_endpoint_region,
                self._kinesis_video_service_name,
                "aws4_request",
            ]
        )
        hashed_canonical_request = hashlib.sha256(
            canonical_request.encode("utf-8")
        ).hexdigest()
        string_to_sign = "\n".join(
            [algorithm, amz_date, credential_scope, hashed_canonical_request]
        )

        signing_key = self.get_signature_key(
            self.input_params.aws_secret_access_key,
            date_stamp,
            self._data_endpoint_region,
            self._kinesis_video_service_name,
        )

        signature = hmac.new(
            signing_key, (string_to_sign).encode("utf-8"), hashlib.sha256
        ).hexdigest()

        authorization_header = (
            algorithm
            + " "
            + "Credential="
            + self.input_params.aws_access_key_id
            + "/"
            + credential_scope
            + ", "
        )
        authorization_header += (
            "SignedHeaders=" + signed_headers + ", " + "Signature=" + signature
        )

        return {
            "Accept": "*/*",
            "Authorization": authorization_header,
            "connection": "keep-alive",
            "content-type": "application/json",
            "transfer-encoding": "chunked",
            "user-agent": "AWS-SDK-KVS/2.0.2 GCC/7.4.0 Linux/4.15.0-46-generic x86_64",
            "x-amz-date": amz_date,
            "x-amzn-fragment-acknowledgment-required": "1",
            "x-amzn-fragment-timecode-type": "ABSOLUTE",
            "x-amzn-producer-start-timestamp": start_timestamp,
            "x-amzn-stream-name": self.input_params.stream_name,
            "Expect": "100-continue",
        }

    def put_media(
        self,
        chunk_size: int = 16000,
    ):
        """
        put_media uploads a video to the Kinesis Video Streams service.
        """
        headers = self._generate_headers()

        response = requests.post(
            self._put_media_endpoint,
            data=ChunkGenerator(
                file_path=self.input_params.video_file_path, chunk_size=chunk_size
            ),
            headers=headers,
            timeout=10,
        )
        self._log.debug("Response: %s", response.text)
        result = []
        for line in response.text.split("\n"):
            if line.startswith("{"):
                data = json.loads(line)
                result.append(data)
        return result
