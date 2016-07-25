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
def msgConstruction(message_dict):
  message_json=json.dumps(message_dict)
  # msg_a
  msg_a = boto.sqs.message.Message()
  msg_a.set_body(message_json)
  # msg_b
  msg_b = boto.sqs.message.Message()
  msg_b.set_body(message_json)
  result = send_msg_ob.send_msg(msg_a, msg_b)
  #print result['result'] 
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
RETURN: N/A
'''
@post('/users')
def create_route():
    ct = request.get_header('content-type')
    if ct != 'application/json':
        return abort(response, 400, [
            "request content-type unacceptable:  body must be "
            "'application/json' (was '{0}')".format(ct)])
    id = request.json["id"] # In JSON, id is already an integer
    name = request.json["name"]
    print "creating id {0}, name {1}\n".format(id, name)
    message_dict = {'op': 'create_user', 'id': id, 'name': name, 'scheme': request.urlparts.scheme, 'netloc':request.urlparts.netloc}
    return msgConstruction(message_dict)

'''
Invokes message to retrieve user by id from backend db
RETURN: N/A
'''
@get('/users/<id>')
def get_id_route(id):
    id = int(id)
    print "Retrieve by id: ".format(id)
    message_dict = {'op': 'retrieve_by_id', 'id': id}
    return msgConstruction(message_dict)

'''
Invokes message to retrieve all users from backend db
RETURN: N/A
'''
@get('/users')
def get_users_route():
    print "Retrieve all users."
    message_dict = {'op': 'retrieve'}
    return msgConstruction(message_dict)

'''
Invokes message to retrieve user by name from backend db
RETURN: N/A
'''
@get('/names/<name>')
def get_name_route(name):
    print "Retrieve by name: ".format(name)
    message_dict = {'op': 'retrieve_by_name', 'name': name}
    return msgConstruction(message_dict)

'''
Invokes message to delete user by id from backend db
RETURN: N/A
'''
@delete('/users/<id>')
def delete_id_route(id):
    id = int(id)
    print "Delete user by id: ".format(id)
    message_dict = {'op': 'delete_by_id', 'id': id }
    return msgConstruction(message_dict)

'''
Invokes message to delete user by name from backend db
RETURN: N/A
'''
@delete('/names/<name>')
def delete_name_route(name):
    print "Deleting user by name {0}\n".format(name)
    message_dict = {'op': 'delete_by_name', 'name': name}
    return msgConstruction(message_dict)

'''
Invokes message to add activity to user in backend db
RETURN: N/A
'''
@put('/users/<id>/activities/<activity>')
def add_activity_route(id, activity):
    id = int(id)
    print "Adding activity to id {0}, activity {1}\n".format(id, activity)
    message_dict = {'op': 'add_activity', 'id': id, 'activity': activity}
    return msgConstruction(message_dict)

'''
Invokes message to delete activity from user in backend db
RETURN: N/A
'''
@delete('/users/<id>/activities/<activity>')
def del_activity_route(id, activity):
    id = int(id)
    print "Deleting activity from id {0}, activity {1}\n".format(id, activity)
    message_dict = {'op': 'del_activity', 'id': id, 'activity': activity}
    return msgConstruction(message_dict)

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
    # create_queue is idempotent---if queue exists, it simply connects to it
    # will change to a3_in_a / a3_in_b
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
history = []


def is_first_response(id):
    # EXTEND:
    # Return True if this message is the first response to a request
    for i in history:
      if i.getIDa() == id or i.getIDb() == id:
          if not i.getFirstResponse(): 
            return True
    return False



def is_second_response(id):
    # EXTEND:
    # Return True if this message is the second response to a request
    for i in history:
      if i.getIDa() == id or i.getIDb() == id:
          if not i.getSecondResponse(): 
            return True
    return False

def get_response_action(id):
    # EXTEND:
    # Return the action for this message
    for i in history:
      if i.getIDa() == id or i.getIDb() == id:
        return i.getAction()

def get_partner_response(id):
    # EXTEND:
    # Return the id of the partner for this message, if any
    for i in history:
      if i.getIDa() == id:
        return i.getIDb()
      elif i.getIDb() == id:
        return i.getIDa()

def mark_first_response(id):
    # EXTEND:
    # Update the data structures to note that the first response has been received
    for i in history:
      if i.getIDa() == id or i.getIDb() == id:
        i.setFirstResponse()


def mark_second_response(id):
    # EXTEND:
    # Update the data structures to note that the second response has been received
    for i in history:
      if i.getIDa() == id or i.getIDb() == id:
        i.setSecondResponse()


def clear_duplicate_response(id):
    # EXTEND:
    # Do anything necessary (if at all) when a duplicate response has been received
    for i in history:
      if i.getIDa() == id or i.getIDb() == id:
        history.remove(i)


class History():
  def __init__(self, id_a, id_b, action):
    self.id_a = id_a
    self.id_b = id_b
    self.action = action
    self.first_response = False
    self.second_response = False
  def getIDa(self):
    return self.id_a
  def getIDb(self):
    return self.id_b
  def getFirstResponse(self):
    return self.first_response
  def getSecondResponse(self):
    return self.second_response
  def setFirstResponse(self):
    self.first_response = True
  def setSecondResponse(self):
    self.second_response = True
  def getAction(self):
    return self.action
    
def set_dup_DS(action, sent_a, sent_b):
  messageAID = sent_a.id
  messageBID = sent_b.id
  history.append(History(messageAID, messageBID, action))