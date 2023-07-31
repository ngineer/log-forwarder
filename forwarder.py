import gzip
import base64
import os
import json
import logging
import boto3

from io import BytesIO, BufferedReader

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def handler(event, context):

  if "awslogs" not in event:
    logger.error(f"Not an awslog")
    return
  
  logger.debug(f"Found awslogs in event")
  parsed = parse(event)
  # logger.debug(f"Log data:{json.dumps(parsed)}")

  logger.debug(f"Sending logs to s3...")
  s3_logger(parsed)
  logger.debug(f"done.")

def parse(event):
  parsed = {}
  metadata = {}
  # Unpack the AWSlogs
  logData = awslogs_handler(event)
  metadata.update({"logStream":logData["logStream"]})
  metadata.update({"logGroup":logData["logGroup"]})

  logEvents = []
  for le in logData.get("logEvents"):
    logEvents.append(json.loads(le["message"]))

  #logger.debug(f"logEvents extracted:{json.dumps(logEvents)}")

  # Get the date time of the first event used to categories
  metadata.update({"streamDateTime":logEvents[0]["requestReceivedTimestamp"]})
  parsed.update({"metadata":metadata})
  parsed.update({"logs": logEvents})

  #logger.debug(f"parsed:{json.dumps(parsed)}")

  return parsed

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
  # get the bucket name from env
  bucket_name = os.getenv("S3_BUCKET_NAME", "")
  # Figure out our s3 key
  cluster_name = logs["metadata"]["logGroup"].split('/')[3]
  stream_name = logs["metadata"]["logStream"]
  dateTimeString = logs["metadata"]["streamDateTime"]

  s3_key = cluster_name + '/' + stream_name + '/' + 'logs_' + dateTimeString + '.json'

  logger.debug(f"Using s3 parameters: {bucket_name}/{s3_key}")

  # Write JSON string
  log_data = json.dumps(logs["logs"], indent=2, default=str)

  # s3 boto resource
  s3_client = boto3.resource("s3", region_name="eu-west-1")
  try:
    s3_bucket = s3_client.Bucket(name=bucket_name)
    s3_bucket.put_object(
      Key = s3_key,
      Body = log_data
    )

  except Exception as e:
    logger.error("Error writing to s3: ", e)

  