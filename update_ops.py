''' Add_activities function for CMPT 474 Assignment 2 '''

from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound

def add_activity(table, id, activity, response):
  try:
    item = table.get_item(id = id，consistent = True)
    response.status = 200
    if item["activities"] != None:
      for x in item["activities"]:
        if x == activity:
          return {"data": {
                  "type": "person",
                  "id": id,
                  "added": []
              }
          }
      item["activities"].add(activity)
      item.save()
      return {"data": {
              "type": "person",
              "id": id,
              "added": [activity]
          }
      }

    p = Item(table, data = {"id": id, "name": item["name"], "activities": {activity}})
    p.save(overwrite = True)
    return {"data": {
            "type": "person",
            "id": id,
            "added": [activity]
        }
    }

  except ItemNotFound as inf:
        response.status = 404 # user not found
        return {
            "errors": [{
                "not_found": {
                    "id": id # "Replace with actual id"
                }
            }]
        }
def del_activity(table, id, activity, response):
  try:
    item = table.get_item(id = id，consistent = True)
    response.status = 200
    if item["activities"] != None:
      if activity in item["activities"]:
        item["activities"].remove(activity)
        item.save()
        return {"data": {
                "type": "person",
                "id": id,
                "deleted": [activity]
            }
        }
    return {"data": {
                "type": "person",
                "id": id,
                "deleted": []
            }
        }

  except ItemNotFound as inf:
    response.status = 404 # user not found
    return {"errors": [{
            "not_found": {
                "id": id
            }
        }]
    }