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
		json_activities = []
		if activities != None:
                        for activity in activities:
                                json_activities.append(activity)
		return {"data": {
					"type": "person",
					"id": id,
					"name": item['name'],
					"activities": json_activities
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
	obj = []
	for item in table.scan():
		obj.append ({
			"type": "users",
			"id": item["id"]
		})
	return {"data": obj}


def retrieve_by_name(table, name, response):
        items = table.scan(name__eq = name)
        for item in items:
                response.status = 200
                id = int(item["id"])
                activities = item['activities']
                json_activities = []
                if activities != None:
                	for activity in activities:
                		json_activities.append(activity)

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
