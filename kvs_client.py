"""
KvsClient is a client for the Kinesis Video Streams service.
"""
import boto3
import datetime
import hashlib
import hmac
import os
import requests
import sys
import time
import logging
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


class KvsClient:
    """
    KvsClient is a client for the Kinesis Video Streams service.
    """

    input_params: InputParams
    _data_endpoint: str
    _data_endpoint_host: str
    _data_endpoint_region: str
    _log: logging.Logger
    _kinesis_video_service_name = "kinesisvideo"

    def __init__(self, input_params: InputParams):
        self.input_params = input_params
        self._kinesis_video_client = boto3.client(self._kinesis_video_service_name)
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
        return self._kinesis_video_client.create_stream(
            StreamName=stream_name,
            DataRetentionInHours=data_retention_in_hours,
        )

    def _get_data_endpoint(
        self,
        stream_name: str,
    ) -> str:
        """
        get_endpoint_boto returns the endpoint for the Kinesis Video Streams service.
        Sample https://s-ca658586.kinesisvideo.ap-southeast-2.amazonaws.com
        """
        return self._kinesis_video_client.get_data_endpoint(
            StreamName=stream_name, APIName="PUT_MEDIA"
        )["DataEndpoint"]

    def sign(self, key, msg):
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
        return endpoint[len("https://") :].split(".")[0]

    @staticmethod
    def _get_region_from_endpoint(endpoint):
        """
        get_region_from_endpoint returns the region from the endpoint.
        """
        if not endpoint.startswith("https://"):
            raise ValueError("Endpoint must start with https://")
        return endpoint[len("https://") :].split(".")[2]

    def _generate_headers(self) -> dict:
        """
        generate_headers returns the headers for the request.
        """

        # Create a date for headers and the credential string
        t = datetime.datetime.utcnow()
        start_tmstp = repr(time.time())

        amz_date = t.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = t.strftime("%Y%m%d")  # Date w/o time, used in credential scope
        canonical_uri = "/putMedia"
        canonical_querystring = ""
        canonical_headers = ""
        canonical_headers += "connection:keep-alive\n"
        canonical_headers += "content-type:application/json\n"
        canonical_headers += "host:" + self._data_endpoint_host + "\n"
        canonical_headers += "transfer-encoding:chunked\n"
        canonical_headers += (
            "user-agent:AWS-SDK-KVS/2.0.2 GCC/7.4.0 Linux/4.15.0-46-generic x86_64\n"
        )
        canonical_headers += "x-amz-date:" + amz_date + "\n"
        canonical_headers += "x-amzn-fragment-acknowledgment-required:1\n"
        canonical_headers += "x-amzn-fragment-timecode-type:ABSOLUTE\n"
        canonical_headers += "x-amzn-producer-start-timestamp:" + start_tmstp + "\n"
        canonical_headers += (
            "x-amzn-stream-name:" + self.input_params.stream_name + "\n"
        )

        signed_headers = "connection;content-type;host;transfer-encoding;user-agent;"
        signed_headers += "x-amz-date;x-amzn-fragment-acknowledgment-required;"
        signed_headers += "x-amzn-fragment-timecode-type;x-amzn-producer-start-timestamp;x-amzn-stream-name"

        canonical_request = (
            "POST\n"
            + canonical_uri
            + "\n"
            + canonical_querystring
            + "\n"
            + canonical_headers
            + "\n"
            + signed_headers
        )
        canonical_request += "\n"
        canonical_request += hashlib.sha256("".encode("utf-8")).hexdigest()

        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = (
            date_stamp
            + "/"
            + self._data_endpoint_region
            + "/"
            + self._kinesis_video_service_name
            + "/"
            + "aws4_request"
        )
        string_to_sign = (
            algorithm
            + "\n"
            + amz_date
            + "\n"
            + credential_scope
            + "\n"
            + hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
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
            "x-amzn-producer-start-timestamp": start_tmstp,
            "x-amzn-stream-name": self.input_params.stream_name,
            "Expect": "100-continue",
        }

    def put_media(self):
        """
        put_media uploads a video to the Kinesis Video Streams service.
        """
        headers = self._generate_headers()
        # pretty print the headers for debugging purposes
        for key, value in headers.items():
            self._log.info("%s: %s", key, value)

        class gen_request_parameters:
            def __init__(self):
                self._data = ""
                if True:
                    localfile = "blue.mkv"  # upload ok
                    with open(localfile, "rb") as image:
                        request_parameters = image.read()
                        self._data = request_parameters
                self._pointer = 0
                self._size = len(self._data)

            def __iter__(self):
                return self

            def __next__(self):
                if self._pointer >= self._size:
                    raise StopIteration  # signals "the end"
                left = self._size - self._pointer
                chunksz = 16000
                if left < 16000:
                    chunksz = left
                pointer_start = self._pointer
                self._pointer += chunksz
                print("Data: chunk size %d" % chunksz)
                return self._data[pointer_start : self._pointer]

        response = requests.post(
            self._data_endpoint,
            data=gen_request_parameters(),
            headers=headers,
            timeout=10,
        )
        self._log.info("Response: %s", response.text)
