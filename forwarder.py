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
DD_CUSTOM_TAGS = "ddtags"
DD_TAGS = ""

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(level=LOGLEVEL)

def handler(event, context):

  logger.info(f"log forwarder starting ...")
  if "awslogs" not in event:
    logger.error(f"Not an awslog")
    return
  
  logger.debug(f"Found awslogs in event")
  #raw_awslogs = awslogs_handler(event)
  #logger.debug(f"Raws awslogs: {json.dumps(raw_awslogs)}")
  
  parsed = parse(event, context)
  metadata = generate_metadata(context)
  #logger.debug(f"Log data:{json.dumps(parsed)}")
  normalised_output = normalize_events(parsed, metadata)

  logger.debug("sending to s3_logger ...")
  s3_logger(normalised_output)
  logger.debug("done.")

  #logger.debug(f"Sending logs to s3...")
  #s3_logger(parsed)
  #logger.debug(f"done.")

def parse(event, context):
  parsed = {}
  metadata = {}
  _metadata = generate_metadata(context)
  # Unpack the AWSlogs
  logData = awslogs_handler(event)
  metadata.update({"logStream":logData["logStream"]})
  metadata.update({"logGroup":logData["logGroup"]})

  aws_attributes = {
    "aws": {
      "awslogs": {
        "logGroup": logData["logGroup"],
        "logStream": logData["logStream"],
        "owner": logData["owner"],
      }
    }
  }

  logEvents = []
  for le in logData.get("logEvents"):
    logEvents.append(le["message"])


    #try:
    # logEvents.append(json.loads(le["message"]))
    #except Exception as json_ex:
    #  logger.error(f"Error appending JSON message to logEvents: ", json_ex)

  #logger.debug(f"logEvents extracted:{json.dumps(logEvents)}")

  # Get the date time of the first event used to categories
  #datetimeString = convert_datetime(logData[0]["timestamp"])
  metadata.update({"streamDateTime": convert_datetime(logData["logEvents"][0]["timestamp"])})
  parsed.update({"metadata":metadata})
  parsed.update({"logs": logEvents})

  #logger.debug(f"parsed:{json.dumps(parsed)}")

  #return parsed

  for log in logData["logEvents"]:
      yield merge_dicts(log, aws_attributes)

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
  cluster_name = logs[0]["aws"]["awslogs"]["logGroup"].split('/')[3]
  stream_name = logs[0]["aws"]["awslogs"]["logStream"]
  dateTimeString = convert_datetime(logs[0]["timestamp"])

  s3_key = cluster_name + '/' + stream_name + '/' + 'logs_' + dateTimeString + '.log'

  logger.debug(f"Using s3 parameters: {bucket_name}/{s3_key}")

  log_data = []
  for log in logs:
    log_data.append(log["message"])

  log_data = '\n'.join(log_data)

  logger.debug(f"log_data: {log_data}")
  # Write JSON string
  #log_data = json.dumps(logs["logs"], indent=2, default=str)

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

def normalize_events(events, metadata):
  normalized = []
  events_counter = 0

  for event in events:
    events_counter += 1
    if isinstance(event, dict):
      normalized.append(merge_dicts(event, metadata))
    elif isinstance(event, str):
      normalized.append(merge_dicts({"message": event}, metadata))
    else:
      # drop this log
      continue

  """Submit count of total events"""
  #lambda_stats.distribution(
  #  "{}.incoming_events".format(DD_FORWARDER_TELEMETRY_NAMESPACE_PREFIX),
  #  events_counter,
  #  tags=get_forwarder_telemetry_tags(),
  #)

  return normalized

def generate_metadata(context):
  metadata = {
    "ddsourcecategory": "aws",
    "aws": {
        "function_version": context.function_version,
        "invoked_function_arn": context.invoked_function_arn,
    },
  }
  # Add custom tags here by adding new value with the following format "key1:value1, key2:value2"  - might be subject to modifications
  dd_custom_tags_data = {
    "forwardername": context.function_name.lower(),
    "forwarder_memorysize": context.memory_limit_in_mb,
    "forwarder_version": "0.0.1",
  }

  metadata[DD_CUSTOM_TAGS] = ",".join(
    filter(
      None,
      [
        DD_TAGS,
        ",".join(
            ["{}:{}".format(k, v) for k, v in dd_custom_tags_data.items()]
        ),
      ],
    )
  )

  return metadata