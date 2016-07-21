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

if __name__ == "__main__":
    args = handle_args()
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

# THIS IS FROM THE ASSIGNMENT 2 DELETE IF NOT NEEDED

# #  You can use the following without modification
# def main():
#     global table
#     try:
#         conn = boto.dynamodb2.connect_to_region(AWS_REGION)
#         if conn == None:
#             sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
#             sys.exit(1)

#         table = boto.dynamodb2.table.Table(TABLE_NAME, connection=conn)
#     except Exception as e:
#         sys.stderr.write("Exception connecting to DynamoDB table {0}\n".format(TABLE_NAME))
#         sys.stderr.write(str(e))
#         sys.exit(1)

#     if len(sys.argv) >= 2:
#         port = int(sys.argv[1])
#     else:
#         port = DEFAULT_PORT

#     app = default_app()
#     run(app, host="localhost", port=port)

# if __name__ == "__main__":
#     main()
    
