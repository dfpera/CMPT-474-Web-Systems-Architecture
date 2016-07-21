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

# Constants
AWS_REGION = "us-west-2"

Q_IN_NAME_BASE = 'a3_in'
Q_OUT_NAME = 'a3_out'

# Respond to health check
@get('/')
def health_check():
		response.status = 200
		return "Healthy"

'''
# EXTEND:
# Define all the other REST operations here ...
@post('/users')
def create_route():
		pass
'''
# def abort(response, status, errors):
#     response.status = status
#     return {"errors": errors}

# @post('/users')
# def create_route():
#     ct = request.get_header('content-type')
#     if ct != 'application/json':
#         return abort(response, 400, [
#             "request content-type unacceptable:  body must be "
#             "'application/json' (was '{0}')".format(ct)])
#     id = request.json["id"] # In JSON, id is already an integer
#     name = request.json["name"]

#     print "creating id {0}, name {1}\n".format(id, name)

#     # Pass the called routine the response object to construct a response from
#     return create_ops.do_create(request, table, id, name, response)

# # retrieve_by_id
# @get('/users/<id>')
# def get_id_route(id):
#     id = int(id) # In URI, id is a string and must be made int
#     print "Retrieving id {0}\n".format(id)

#     return retrieve_ops.retrieve_by_id(table, id, response)

# # retrieve_users
# @get('/users')
# def get_users_route():
#     print "Retrieving all users\n"

#     return retrieve_ops.retrieve_users(table, response)

# # retrieve_by_name
# @get('/names/<name>')
# def get_name_route(name):
#     print "Retrieving name {0}\n".format(name)
    
#     return retrieve_ops.retrieve_by_name(table, name, response)

# # delete_by_id
# @delete('/users/<id>')
# def delete_id_route(id):
#     id = int(id)

#     print "Deleting id {0}\n".format(id)

#     return delete_ops.delete_by_id(table, id, response)

# # delete_by_name
# @delete('/names/<name>')
# def delete_name_route(name):
#     print "Deleting name {0}\n".format(name)
#     return delete_ops.delete_by_name(table, name, response)

# # add_activity
# @put('/users/<id>/activities/<activity>')
# def add_activity_route(id, activity):
#     id = int(id)
#     print "adding activity for id {0}, activity {1}\n".format(id, activity)
    
#     return update_ops.add_activity(table, id, activity, response)

# # del_activity
# @delete('/users/<id>/activities/<activity>') # mark 1, 06/17/16
# def del_activity_route(id, activity):
#     id = int(id)
#     print "deleting activity for id {0}, activity {1}\n".format(id, activity)
    
#     return update_ops.del_activity(table, id, activity, response)

# a = {'msg_id':id}#need more attributes

# a_jason_str = json.dumps(a)#convert to json format

# #construct two copies of the request message, one for each queue
# msg_a = boto.sqs.message.Message()
# msg_a.set_body(a_jason_str)
# msg_b = boto.sqs.message.Message()
# msg_b.set_body(a_jason_str)

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

def create_q(name):
		try:
				conn = boto.sqs.connect_to_region(AWS_REGION)
				if conn == None:
						sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
						sys.exit(1)

				# create_queue is idempotent---if queue exists, it simply connects to it
				return conn.create_queue(name)
		except Exception as e:
				sys.stderr.write("Exception connecting to SQS\n")
				sys.stderr.write(str(e))
				sys.exit(1)

a3_in_a = conn.create_q(Q_IN_NAME_BASE)
a3_in_b = conn.create_q(Q_IN_NAME_BASE)

msg = boto.sqs.message.Message()#construct a single response message
msg.set_body(a_jason_str)

q_out = {}
q_out.write(msg)#store output reference in q_out

def write_to_queues(msg_a, msg_b):
		# EXTEND:
		# Send msg_a to a3_in_a and msg-b to a3_in_b

		send_msg_ob.send_msg(msg_a, msg_b)



		pass

'''
	 EXTEND:
	 Manage the data structures for detecting the first and second
	 responses and any duplicate responses.
'''

# Define any necessary data structures globally here

def is_first_response(id):
		# EXTEND:
		# Return True if this message is the first response to a request

		pass

def is_second_response(id):
		# EXTEND:
		# Return True if this message is the second response to a request
		pass

def get_response_action(id):
		# EXTEND:
		# Return the action for this message
		pass

def get_partner_response(id):
		# EXTEND:
		# Return the id of the partner for this message, if any
		pass

def mark_first_response(id):
		# EXTEND:
		# Update the data structures to note that the first response has been received
		pass

def mark_second_response(id):
		# EXTEND:
		# Update the data structures to note that the second response has been received
		pass

def clear_duplicate_response(id):
		# EXTEND:
		# Do anything necessary (if at all) when a duplicate response has been received
		pass

def set_dup_DS(action, sent_a, sent_b):
		'''
			 EXTEND:
			 Set up the data structures to identify and detect duplicates
			 action: The action to perform on receipt of the response.
							 Opaque data type: Simply save it, do not interpret it.
			 sent_a: The boto.sqs.message.Message() that was sent to a3_in_a.
			 sent_b: The boto.sqs.message.Message() that was sent to a3_in_b.
			 
							 The .id field of each of these is the message ID assigned
							 by SQS to each message.  These ids will be in the
							 msg_id attribute of the JSON object returned by the
							 response from the backend code that you write.
		'''
		pass
