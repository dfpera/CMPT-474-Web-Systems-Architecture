''' Retrieve operations for CMPT 474 Assignment 2 '''

from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound
 #!/usr/local/bin/python
import json
class SetEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, set):
			return list(obj)
		if isinstance(obj, Something):
			return 'CustomSomethingRepresentation'
		return json.JSONEncoder.default(self, obj)

def retrieve_by_id(table, id, response):
	try:
		item = table.get_item(id=id)
		response.status = 200 # item found
		activities = item['activities']
		if activities == None:
			activities = []
		return {"data": {
					"type": "person",
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
	print "Retrieve users"
	item = table.scan()
	for i in item:
		obj = {
			"id": "users",
			"name": i['name']
		}
	return (json.dumps(obj,indent=4))		

def retrieve_by_name(table, name, response):
        items = table.scan(name__eq = name)
        for item in items:
                response.status = 200
                id = int(item["id"])
                activities = item['activities']
                if activities == None:
                	json_activities = []
                else:
                	json_activities = json.dumps(activities, cls = SetEncoder)    
                return {"data": {
                        "type": "person",
                        "id": id, 
                        "name": name,
                        "activities" : json_activities
                                        }
                        }
        response.status = 404 # "Delete"
        return {"errors": [{
                "not_found": {
                        "name": name
                        }
                }]
                }
