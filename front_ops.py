'''
   Ops for the frontend of Assignment 3, Summer 2016 CMPT 474.
'''

# Standard library packages
import json

# Installed packages
import boto.sqs

# Imports of unqualified names
from bottle import post, get, put, delete, request, response

# Local modules
import SendMsg
from gevent.event import AsyncResult

# Constants
AWS_REGION = "us-west-2"

Q_IN_NAME_BASE = 'a3_in'
Q_OUT_NAME = 'a3_out'
q_out = None
histories = []
seq_num = 0

try:
    conn = boto.sqs.connect_to_region(AWS_REGION)
    if conn == None:
        sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
        sys.exit(1)
    # create_queue is idempotent---if queue exists, it simply connects to it
    q_out = conn.create_queue(Q_OUT_NAME)
except Exception as e:
    sys.stderr.write("Exception connecting to SQS\n")
    sys.stderr.write(str(e))
    sys.exit(1)

# Respond to health check
@get('/')
def health_check():
    response.status = 200
    return "Healthy"

'''
Construct message msg_a and msg_b to be sent to queue a3_in_a and a3_in_b
PARAM: Python dictionary of operation to be perfomed
RETURN: Python object response from backend.py 
'''
def msg_construction(message_dict):
  message_json=json.dumps(message_dict)
  msg_a = boto.sqs.message.Message()
  msg_a.set_body(message_json)
  msg_b = boto.sqs.message.Message()
  msg_b.set_body(message_json)
  result = send_msg_ob.send_msg(msg_a, msg_b)
  return make_response(result, response)

def make_response(result, response):
  response.status = result['status']
  return result['result'] 

'''
# EXTEND:
# Define all the other REST operations here ...
'''

'''
Invokes message to create user in the backend db
'''
@post('/users')
def create_route():
    global seq_num
    ct = request.get_header('content-type')
    if ct != 'application/json':
        return abort(response, 400, [
            "request content-type unacceptable:  body must be "
            "'application/json' (was '{0}')".format(ct)])
    id = request.json["id"] # In JSON, id is already an integer
    name = request.json["name"]
    print "creating id {0}, name {1}\n".format(id, name)
    seq_num += 1
    message_dict = {'op': 'create_user', 'id': id, 'name': name, 'scheme': request.urlparts.scheme, 'netloc':request.urlparts.netloc, 'opnum':seq_num.value}
    return msg_construction(message_dict)

'''
Invokes message to retrieve user by id from backend db
'''
@get('/users/<id>')
def get_id_route(id):
    global seq_num
    id = int(id)
    print "Retrieve by id: {0}\n".format(id)
    seq_num += 1
    message_dict = {'op': 'retrieve_by_id', 'id': id, 'opnum':seq_num.value}
    return msg_construction(message_dict)

'''
Invokes message to retrieve all users from backend db
'''
@get('/users')
def get_users_route():
    global seq_num
    print "Retrieve all users."
    seq_num += 1
    message_dict = {'op': 'retrieve', 'opnum':seq_num.value}
    return msg_construction(message_dict)

'''
Invokes message to retrieve user by name from backend db
'''
@get('/names/<name>')
def get_name_route(name):
    global seq_num
    print "Retrieve by name: {0}\n".format(name)
    seq_num += 1
    message_dict = {'op': 'retrieve_by_name', 'name': name, 'opnum':seq_num.value}
    return msg_construction(message_dict)

'''
Invokes message to delete user by id from backend db
'''
@delete('/users/<id>')
def delete_id_route(id):
    global seq_num
    id = int(id)
    print "Delete user by id: {0}\n".format(id)
    seq_num += 1
    message_dict = {'op': 'delete_by_id', 'id': id, 'opnum':seq_num.value }
    return msg_construction(message_dict)

'''
Invokes message to delete user by name from backend db
'''
@delete('/names/<name>')
def delete_name_route(name):
    global seq_num
    print "Deleting user by name {0}\n".format(name)
    seq_num += 1
    message_dict = {'op': 'delete_by_name', 'name': name, 'opnum':seq_num.value}
    return msg_construction(message_dict)

'''
Invokes message to add activity to user in backend db
'''
@put('/users/<id>/activities/<activity>')
def add_activity_route(id, activity):
    global seq_num
    id = int(id)
    print "Adding activity to id {0}, activity {1}\n".format(id, activity)
    seq_num += 1
    message_dict = {'op': 'add_activity', 'id': id, 'activity': activity, 'opnum':seq_num.value}
    return msg_construction(message_dict)

'''
Invokes message to delete activity from user in backend db
'''
@delete('/users/<id>/activities/<activity>')
def del_activity_route(id, activity):
    global seq_num
    id = int(id)
    print "Deleting activity from id {0}, activity {1}\n".format(id, activity)
    seq_num += 1
    message_dict = {'op': 'del_activity', 'id': id, 'activity': activity, 'opnum':seq_num.value}
    return msg_construction(message_dict)

'''
   Boilerplate: Do not modify the following function. It
   is called by frontend.py to inject the names of the two
   routines you write in this module into the SendMsg
   object.  See the comments in SendMsg.py for why
   we need to use this awkward construction.

   This function creates the global object send_msg_ob.

   To send messages to the two backend instances, call

       send_msg_ob.send_msg(msg_a, msg_b)

   where 

       msg_a is the boto.message.Message() you wish to send to a3_in_a.
       msg_b is the boto.message.Message() you wish to send to a3_in_b.

       These must be *distinct objects*. Their contents should be identical.
'''
def set_send_msg(send_msg_ob_p):
    global send_msg_ob
    send_msg_ob = send_msg_ob_p.setup(write_to_queues, set_dup_DS)

'''
   EXTEND:
   Set up the input queues and output queue here
   The output queue reference must be stored in the variable q_out
'''

def write_to_queues(msg_a, msg_b):
  try:
    conn = boto.sqs.connect_to_region(AWS_REGION)
    if conn == None:
        sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
        sys.exit(1)
    qin_a = conn.create_queue("a3_in_a")
    qin_b = conn.create_queue("a3_in_b")
    qin_a.write(msg_a)
    qin_b.write(msg_b)
  except Exception as e:
    sys.stderr.write("Exception connecting to SQS\n")
    sys.stderr.write(str(e))
    sys.exit(1)

'''
   EXTEND:
   Manage the data structures for detecting the first and second
   responses and any duplicate responses.
'''
def is_first_response(id):
    # EXTEND:
    # Return True if this message is the first response to a request
    for history in histories:
      if history.get_id_a() == id or history.get_id_b() == id:
        if not history.get_firstResponse(): 
          return True
    return False



def is_second_response(id):
    # EXTEND:
    # Return True if this message is the second response to a request
    for history in histories:
      if not history.get_secondResponse():
        if get_partner_response(history.get_firstResponse()) == id:
          return True
    return False

def get_response_action(id):
    # EXTEND:
    # Return the action for this message
    for history in histories:
      if history.get_id_a() == id or history.get_id_b() == id:
        return history.getAction()

def get_partner_response(id):
    # EXTEND:
    # Return the id of the partner for this message, if any
    for history in histories:
      if history.get_id_a() == id:
        return history.get_id_b()
      elif history.get_id_b() == id:
        return history.get_id_a()

def mark_first_response(id):
    # EXTEND:
    # Update the data structures to note that the first response has been received
    for history in histories:
      if history.get_id_a() == id or history.get_id_b() == id:
        history.set_firstResponse(id)
        break
        


def mark_second_response(id):
    # EXTEND:
    # Update the data structures to note that the second response has been received
    for history in histories:
      if history.get_id_a() == id or history.get_id_b() == id:
        history.set_secondResponse(id)
        break

def clear_duplicate_response(id):
    # EXTEND:
    # Do anything necessary (if at all) when a duplicate response has been received
    for history in histories:
      if history.get_id_a() == id or history.get_id_b() == id:
        if history.get_secondResponse():
          histories.remove(history)
          break

def setup_op_counter():
    global seq_num
    zkcl = send_msg_ob.get_zkcl()
    if not zkcl.exists('/SeqNum'):
        zkcl.create('/SeqNum', "0")
    else:
        zkcl.set('/SeqNum', "0")
    seq_num = zkcl.Counter('/SeqNum')


class History():
  def __init__(self, id_a, id_b, action):
    self.id_a = id_a
    self.id_b = id_b
    self.action = action
    self.first_response = None
    self.second_response = None
  def get_id_a(self):
    return self.id_a
  def get_id_b(self):
    return self.id_b
  def get_firstResponse(self):
    return self.first_response
  def get_secondResponse(self):
    return self.second_response
  def set_firstResponse(self, id):
    self.first_response = id
  def set_secondResponse(self, id):
    self.second_response = id
  def getAction(self):
    return self.action
    
def set_dup_DS(action, sent_a, sent_b):
  message_IDa = sent_a.id
  message_IDb = sent_b.id
  histories.append(History(message_IDa, message_IDb, action))

'''
  Define a Zookeeper operations counter as global variable seq_num
'''



