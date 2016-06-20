''' Retrieve operations for CMPT 474 Assignment 2 '''

from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound

def retrieve_by_id(table, id, response):
	print "Initialize Retrieve"
	try:
		item = table.get_item(id=id)
		response.status = 200 # item found
		activities = item['activities']
		if activities == None:
			activities = []
		print "User found"
		return {"data": {
					"type": item['type'],
					"id": id,
					"name": item['name'],
					"activities": activities
				}
			}
	except ItemNotFound as inf:
		print "User not found"
		response.status = 404 # item not found
		return {"errors": [{
				 "not_found": {
						"id": id
					}
				}]
			}
#
def retrieve_users(table, response):
	print "Retrieve users not yet implemented"
	response.status = 501
	return {"errors": [{
		"retrieve users not implemented": " "
		}]}

def retrieve_by_name(table, name, response):
	print "Retrieve by name not yet implented"
	response.status = 501
	return {"errors": [{
		"retrieve by name not implemented": {"name": name}
		}]}
