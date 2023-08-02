import gzip
import base64
import os
import json
import logging
import boto3

from io import BytesIO, BufferedReader
from datetime import datetime

LOGLEVEL = os.getenv("LOGLEVEL", "INFO").upper()
AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(level=LOGLEVEL)

def handler(event, context):
  logger.info(f"log forwarder starting ...")
  
  if "awslogs" not in event:
    logger.error(f"Not an awslog event")
    return
  
  parsed = awslogs_handler(event)
  metadata = generate_metadata(parsed)
  logger.debug(f"metadata: {metadata}")
  normalised = normalise_events(parsed["logEvents"], metadata)

  logger.debug("sending to s3_logger ...")
  s3_logger(normalised)
  logger.debug("done.")

# Handle CloudWatch logs
def awslogs_handler(event):
  # Get logs
  with gzip.GzipFile(
    fileobj=BytesIO(base64.b64decode(event["awslogs"]["data"]))
  ) as decompress_stream:
    # Reading line by line avoid a bug where gzip would take a very long
    # time (>5min) for file around 60MB gzipped
    data = b"".join(BufferedReader(decompress_stream))
  logs = json.loads(data)

  return logs

# Send log to s3 bucket
def s3_logger(logs):

  logGroup = logs[0]["awslogs"]["logGroup"]
  # get the bucket name from env
  # list used for dataplane logs
  for source in [
    '/forgerock'
  ]:
    if logGroup.endswith(source):
      bucket_name = os.getenv("S3_DATAPLANE_BUCKET_NAME", "")
    else:
      bucket_name = os.getenv("S3_CONTROLPLANE_BUCKET_NAME", "")
  # Figure out our s3 key
  if logGroup.startswith('/aws/eks/containerinsights'):
    cluster_name = logGroup.split('/')[4]
  else:
    cluster_name = logGroup.split('/')[3]
  stream_name = logs[0]["awslogs"]["logStream"]
  dateTimeString = convert_datetime(logs[0]["timestamp"])

  s3_key = cluster_name + '/' + stream_name + '/' + 'logs_' + dateTimeString + '.log'

  logger.debug(f"Using s3 parameters: {bucket_name}/{s3_key}")

  log_data = []
  for log in logs:
    log_data.append(log["message"])

  log_data = '\n'.join(log_data)

  logger.debug(f"log_data: {log_data}")

  # s3 boto resource
  s3_client = boto3.resource("s3", region_name=AWS_REGION)
  try:
    s3_bucket = s3_client.Bucket(name=bucket_name)
    s3_bucket.put_object(
      Key = s3_key,
      Body = log_data
    )

  except Exception as e:
    logger.error(f"Error writing to s3: {e}")

def convert_datetime(ts):
  return datetime.fromtimestamp(int(ts) / 1000).strftime('%Y-%m-%d_%H:%M')

def merge_dicts(a, b, path=None):
  if path is None:
    path = []
  for key in b:
    if key in a:
      if isinstance(a[key], dict) and isinstance(b[key], dict):
        merge_dicts(a[key], b[key], path + [str(key)])
      elif a[key] == b[key]:
        pass  # same leaf value
      else:
        raise Exception(
          "Conflict while merging metadatas and the log entry at %s"
          % ".".join(path + [str(key)])
        )
    else:
      a[key] = b[key]
  return a

def normalise_events(events, metadata):
  normalised = []

  for event in events:
    if isinstance(event, dict):
      normalised.append(merge_dicts(event, metadata))
    elif isinstance(event, str):
      normalised.append(merge_dicts({"message": event}, metadata))
    else:
      continue

  return normalised

def generate_metadata(logData):
  metadata = {
    "awslogs": {
      "logGroup": logData["logGroup"],
      "logStream": logData["logStream"],
      "owner": logData["owner"],
    }
  }

  return metadata