import boto3
import datetime
import hashlib
import hmac
import os
import requests
import sys
import time


def get_endpoint_boto():
    client = boto3.client("kinesisvideo")
    response = client.get_data_endpoint(
        StreamName=os.environ.get("STREAM_NAME"),
        APIName="PUT_MEDIA",
    )
    endpoint = response.get("DataEndpoint", None)
    print("endpoint %s" % endpoint)
    if endpoint is None:
        raise Exception("endpoint none")
    return endpoint


def sign(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def get_signature_key(key, date_stamp, regionName, serviceName):
    kDate = sign(("AWS4" + key).encode("utf-8"), date_stamp)
    kRegion = sign(kDate, regionName)
    kService = sign(kRegion, serviceName)
    kSigning = sign(kService, "aws4_request")
    return kSigning


def get_host_from_endpoint(endpoint):
    if not endpoint.startswith("https://"):
        return None
    retv = endpoint[len("https://") :]
    return str(retv)


def get_region_from_endpoint(endpoint):
    if not endpoint.startswith("https://"):
        return None
    retv = endpoint[len("https://") :].split(".")[2]
    return str(retv)


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


# ************* REQUEST VALUES *************
endpoint = get_endpoint_boto()

method = "POST"
service = "kinesisvideo"
host = get_host_from_endpoint(endpoint)
region = get_region_from_endpoint(endpoint)
##endpoint = 'https://**<the endpoint you get with get_data_endpoint>**/PutMedia'

endpoint += "/putMedia"

# POST requests use a content type header. For DynamoDB,
# the content is JSON.
content_type = "application/json"
start_tmstp = repr(time.time())

# Read AWS access key from env. variables or configuration file. Best practice is NOT
# to embed credentials in code.
access_key = os.environ.get("AWS_ACCESS_KEY_ID")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
if access_key is None or secret_key is None:
    print("No access key is available.")
    sys.exit()

# Create a date for headers and the credential string
t = datetime.datetime.utcnow()
amz_date = t.strftime("%Y%m%dT%H%M%SZ")
date_stamp = t.strftime("%Y%m%d")  # Date w/o time, used in credential scope
canonical_uri = "/putMedia"  # endpoint[len('https://'):]
canonical_querystring = ""
canonical_headers = ""
canonical_headers += "connection:keep-alive\n"
canonical_headers += "content-type:application/json\n"
canonical_headers += "host:" + host + "\n"
canonical_headers += "transfer-encoding:chunked\n"
canonical_headers += (
    "user-agent:AWS-SDK-KVS/2.0.2 GCC/7.4.0 Linux/4.15.0-46-generic x86_64\n"
)
canonical_headers += "x-amz-date:" + amz_date + "\n"
canonical_headers += "x-amzn-fragment-acknowledgment-required:1\n"
canonical_headers += "x-amzn-fragment-timecode-type:ABSOLUTE\n"
canonical_headers += "x-amzn-producer-start-timestamp:" + start_tmstp + "\n"
canonical_headers += "x-amzn-stream-name:" + os.environ.get("STREAM_NAME") + "\n"

signed_headers = "connection;content-type;host;transfer-encoding;user-agent;"
signed_headers += "x-amz-date;x-amzn-fragment-acknowledgment-required;"
signed_headers += (
    "x-amzn-fragment-timecode-type;x-amzn-producer-start-timestamp;x-amzn-stream-name"
)

canonical_request = (
    method
    + "\n"
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
credential_scope = date_stamp + "/" + region + "/" + service + "/" + "aws4_request"
string_to_sign = (
    algorithm
    + "\n"
    + amz_date
    + "\n"
    + credential_scope
    + "\n"
    + hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
)

signing_key = get_signature_key(secret_key, date_stamp, region, service)

signature = hmac.new(
    signing_key, (string_to_sign).encode("utf-8"), hashlib.sha256
).hexdigest()

authorization_header = (
    algorithm + " " + "Credential=" + access_key + "/" + credential_scope + ", "
)
authorization_header += (
    "SignedHeaders=" + signed_headers + ", " + "Signature=" + signature
)

headers = {
    "Accept": "*/*",
    "Authorization": authorization_header,
    "connection": "keep-alive",
    "content-type": content_type,
    "transfer-encoding": "chunked",
    "user-agent": "AWS-SDK-KVS/2.0.2 GCC/7.4.0 Linux/4.15.0-46-generic x86_64",
    "x-amz-date": amz_date,
    "x-amzn-fragment-acknowledgment-required": "1",
    "x-amzn-fragment-timecode-type": "ABSOLUTE",
    "x-amzn-producer-start-timestamp": start_tmstp,
    "x-amzn-stream-name": os.environ.get("STREAM_NAME"),
    "Expect": "100-continue",
}

# print(headers)
import logging

logging.basicConfig(level=logging.INFO)

for key, value in headers.items():
    logging.info("%s: %s", key, value)

r = requests.post(
    endpoint,
    data=gen_request_parameters(),
    headers=headers,
    timeout=10,
)

print(r.text)
