#!/usr/bin/env python

'''
  Back end DB server for Assignment 3, CMPT 474.
'''

# Library packages
import argparse
import json
import sys
import time

# Installed packages
import boto.dynamodb2
import boto.dynamodb2.table
import boto.sqs
# Local import
import create_ops

AWS_REGION = "us-west-2"
TABLE_NAME_BASE = "activities"
Q_IN_NAME_BASE = "a3_back_in"
Q_OUT_NAME = "a3_out"

MAX_TIME_S = 3600 # One hour
MAX_WAIT_S = 20 # SQS sets max. of 20 s
DEFAULT_VIS_TIMEOUT_S = 60


def handle_args():
    argp = argparse.ArgumentParser(
        description="Backend for simple database")
    argp.add_argument('suffix', help="Suffix for queue base ({0}) and table base ({1})".format(Q_IN_NAME_BASE, TABLE_NAME_BASE))
    return argp.parse_args()

def connectQueue(name):
  try:
        conn = boto.sqs.connect_to_region(AWS_REGION)
        print "sqs successful"
        if conn == None:
            sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
            sys.exit(1)
        print "yes"
        q_in = conn.get_queue(Q_IN_NAME_BASE)
        print "q_in successfull"
        q_out = conn.get_queue(Q_OUT_NAME)
        print "q_out successfull"
        return (q_in, q_out)
  except Exception as e:
        sys.stderr.write("Exception connecting to queue {0}\n".format(Q_IN_NAME_BASE))
        sys.stderr.write(str(e))
        sys.exit(1)
def connectTable(name):
  try:
        conn = boto.dynamodb2.connect_to_region(AWS_REGION)
        print "tabel"
        if conn == None:
            sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
            sys.exit(1)
        table = boto.dynamodb2.table.Table(TABLE_NAME_BASE, connection=conn)
        print "table successfull"
        return table
  except Exception as e:
        sys.stderr.write("Exception connecting to DynamoDB table {0}\n".format(TABLE_NAME_BASE))
        sys.stderr.write(str(e))
        sys.exit(1)

if __name__ == "__main__":
    args = handle_args()
    q_in, q_out= connectQueue(args.suffix)
    table = connectTable(args.suffix)
    wait_start = time.time()
    while True:
      msg_in = q_in.read(wait_time_seconds=MAX_WAIT_S, visibility_timeout=DEFAULT_VIS_TIMEOUT_S)
      print "msg_in",msg_in.get_body()
      if msg_in:
          body = json.loads(msg_in.get_body())
          msg_id = body['msg_id']
          msg_op = body['op']
          msg_response = None
          if msg_op == "create_user":
            msg_name = body['name']
            msg_request = body['request']
            msg_response = create_ops.create_ops.do_create(request, table, id, name, response)


          msg = boto.sqs.message.Message()
          msg_response_json = json.dumps(msg_response)
          msg.set_body(msg_response_json)
          q_out.write(msg)
          wait_start = time.time()
      elif time.time() - wait_start > MAX_TIME_S:
          print "\nNo messages on input queue for {0} seconds. Server no longer reading response queue {1}.".format(MAX_TIME_S, q_out.name)
      else:
          pass

    '''
       EXTEND:
       
       After the above statement, args.suffix holds the suffix to use
       for the input queue and the DynamoDB table.

       This main routine must be extended to:
       1. Connect to the appropriate queues
       2. Open the appropriate table
       3. Go into an infinite loop that
          - reads a requeat from the SQS queue, if available
          - handles the request idempotently if it is a duplicate
          - if this is the first time for this request
            * do the requested database operation
            * record the message id and response
            * put the response on the output queue
    '''
