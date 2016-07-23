''' Retrieve operations for CMPT 474 Assignment 2 '''

from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound
 #!/usr/local/bin/python
import json

def retrieve_by_id(table, id, response):
  try:
    item = table.get_item(id = id)
    response.status = 200 # item found
    activities = item['activities']
    json_activities = []
    if activities != None: # only if the user has activities
      for activity in activities:
        json_activities.append(activity)
    return {"data": {
          "type": "person",
          "id": id,
          "name": item["name"],
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

def retrieve_users(table, response):
  obj = []
  for item in table.scan():
    obj.append ({
      "type": "users",
      "id": int(item["id"])
    })
  return {"data": obj}

def retrieve_by_name(table, name, response):
        items = table.scan(name__eq = name)
        for item in items:
                response.status = 200
                id = int(item["id"])
                activities = item["activities"]
                json_activities = []
                if activities != None: # only if the user has activities
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
