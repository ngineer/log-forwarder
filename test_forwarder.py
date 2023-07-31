import unittest
import gzip
import base64
import os

def create_cw_log(payload):
  gz_data = gzip.compress(bytes(payload, encoding="utf-8"))
  encoded = base64.b64encode(gz_data).decode("utf-8")

  return encoded

class Context:
    function_version = 0
    invoked_function_arn = "arn:aws:lambda:eu-west-2:601427279990:function:test-log-forwarder-function"
    function_name = "test-log-forwarder-function"
    memory_limit_in_mb = "10"

class test_forwarder(unittest.TestCase):
  def test_log_fowarder(self):
    context = Context()

    # Get our test data
    my_path = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(my_path, "examples/cw_events.json")

    with open(
        path,
        "r",
    ) as input_file:
        input_data = input_file.read()

    event = {"awslogs": {"data": create_cw_log(input_data)}}