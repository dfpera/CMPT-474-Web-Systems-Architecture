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
import delete_ops
import update_ops
import retrieve_ops
from bottle import response
AWS_REGION = "us-west-2"
TABLE_NAME_BASE = "activities"
Q_IN_NAME_BASE = "a3_back_in"
Q_OUT_NAME = "a3_out"

MAX_TIME_S = 3600 # One hour
MAX_WAIT_S = 20 # SQS sets max. of 20 s
DEFAULT_VIS_TIMEOUT_S = 60
ID_Stored = []
has_stored_id = False
opnums = []
seq_num = 1

class ID_Backend():
  def __init__(self, id ,response, status):
    self.id = id
    self.response = response
    self.status = status
  def get_id(self):
    return self.id
  def get_response(self):
    return self.response
  def get_status(self):
    return self.status

def handle_args():
    argp = argparse.ArgumentParser(
        description="Backend for simple database")
    argp.add_argument('suffix', help="Suffix for queue base ({0}) and table base ({1})".format(Q_IN_NAME_BASE, TABLE_NAME_BASE))
    return argp.parse_args()

def insertNewOperation(info):
  global opnums
  opnums.append(info)
  opnums.sorted(opnums, key=lambda k:k['opnum'])

def run_message(body):
  msg_id = body['msg_id']
  msg_response = None
  for stored in ID_Stored:
    if stored.get_id() == msg_id: 
      msg_response = stored.get_response()
      response.status = stored.get_status()
      has_stored_id = True
      break
  if not has_stored_id: 
    msg_op = body['op']
    if msg_op == "create_user":
      msg_user_id = body['id']
      msg_name = body['name']
      msg_scheme = body['scheme']
      msg_netloc = body['netloc'] 
      msg_response = create_ops.do_create(msg_scheme, msg_netloc,table, msg_user_id, msg_name, response)
      ID_Stored.append(ID_Backend(msg_id,msg_response, response.status))
    elif msg_op == "delete_by_id":
      msg_user_id = body['id']
      msg_response = delete_ops.delete_by_id(table, msg_user_id, response)
      ID_Stored.append(ID_Backend(msg_id,msg_response, response.status))
    elif msg_op == "delete_by_name":
      msg_name = body['name']
      msg_response = delete_ops.delete_by_name(table, msg_name, response)
      ID_Stored.append(ID_Backend(msg_id,msg_response,response.status))
    elif msg_op == "retrieve_by_id":
      msg_user_id = body['id']
      msg_response = retrieve_ops.retrieve_by_id(table, msg_user_id, response)
      ID_Stored.append(ID_Backend(msg_id,msg_response,response.status))
    elif msg_op == "retrieve_by_name":
      msg_name = body['name']
      msg_response = retrieve_ops.retrieve_by_name(table, msg_name, response)
      ID_Stored.append(ID_Backend(msg_id,msg_response, response.status))
    elif msg_op == "retrieve":
      msg_response = retrieve_ops.retrieve_users(table, response)
      ID_Stored.append(ID_Backend(msg_id,msg_response, response.status))
    elif msg_op == "add_activity":
      msg_user_id = body['id']
      msg_activity = body['activity']
      msg_response = update_ops.add_activity(table, msg_user_id, msg_activity, response)
      ID_Stored.append(ID_Backend(msg_id,msg_response, response.status))
    elif msg_op == "del_activity":
      msg_user_id = body['id']
      msg_activity = body['activity']
      msg_response = update_ops.del_activity(table, msg_user_id, msg_activity, response)
      ID_Stored.append(ID_Backend(msg_id,msg_response, response.status))
  msg_result = {}
  msg = boto.sqs.message.Message()
  msg_result['result'] = msg_response
  msg_result['status'] = response.status
  msg_result['msg_id'] = msg_id
  print "msg_result", msg_result
  msg_result_json = json.dumps(msg_result)
  msg.set_body(msg_result_json)
  q_out.write(msg)
  wait_start = time.time()
  has_stored_id = False


def connect_queue(name):
  try:
        conn = boto.sqs.connect_to_region(AWS_REGION)
        if conn == None:
            sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
            sys.exit(1)
        q_in = conn.create_queue(Q_IN_NAME_BASE + name)
        q_out = conn.create_queue(Q_OUT_NAME)
        return (q_in, q_out)
  except Exception as e:
        sys.stderr.write("Exception connecting to queue {0}\n".format(Q_IN_NAME_BASE + name))
        sys.stderr.write(str(e))
        sys.exit(1)

def connect_table(name):
  try:
        conn = boto.dynamodb2.connect_to_region(AWS_REGION)
        if conn == None:
            sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
            sys.exit(1)
        table = boto.dynamodb2.table.Table(TABLE_NAME_BASE + name, connection=conn)
        return table
  except Exception as e:
        sys.stderr.write("Exception connecting to DynamoDB table {0}\n".format(TABLE_NAME_BASE + name))
        sys.stderr.write(str(e))
        sys.exit(1)

if __name__ == "__main__":
    global opnums
    global seq_num
    args = handle_args()
    q_in, q_out= connect_queue(args.suffix)
    table = connect_table(args.suffix)
    wait_start = time.time()
    while True:
      msg_in = q_in.read(wait_time_seconds=MAX_WAIT_S, visibility_timeout=DEFAULT_VIS_TIMEOUT_S)
      if msg_in:
        body = json.loads(msg_in.get_body())
        insertNewOperation(body)
        q_in.delete_message(msg_in)
        get_first_msg = opnums[0]
        opnum_msg = get_first_msg["opnum"]
        while seq_num >=  opnum_msg:
          if seq_num == get_first_msg["opnum"]:
            run_message(get_first_msg)
            opnums.pop(0)
            seq_num += 1
          elif seq_num > get_first_msg["opnum"]:
            run_message(get_first_msg)
            opnums.pop(0)
          get_first_msg = opnums[0]
          opnum_msg = get_first_msg["opnum"]
      elif time.time() - wait_start > MAX_TIME_S:
          print "\nNo messages on input queue for {0} seconds. Server no longer reading response queue {1}.".format(MAX_TIME_S, q_out.name)
          break
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
            * put the message id and response on the output queue
    '''
