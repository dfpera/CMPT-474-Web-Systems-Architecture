#!/bin/sh
if [ $# -ne 1 ]
then
  echo "Must specify IP address"
  exit 1
fi

KEY_FILE=

scp -i $KEY_FILE backend.py create_ops.py delete_ops.py duplicator.py front_ops.py frontend.py front_ops.py retrieve_ops.py SendMsg.py update_ops.py counterlast.py kazooclientlast.py ubuntu@$1:/home/ubuntu
