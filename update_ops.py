''' Add_activities function for CMPT 474 Assignment 2 '''

from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound

def add_activity(table, id, activity, response):
  try:
    item = table.get_item(id=id)
    response.status = 200
    json_activity = []
    p = Item(table, data={'id': id, 'name': item['name'], 'activities': {activity}})
    json_activity.append(activity)
    print "type: "+ str(type(activity))
    print "type: "+ str(type(json_activity))
    p.save(overwrite=True)
    return {"data": {
    "type": "person",
    "id": id,
    "added": [activity]
    }
    }

  except ItemNotFound as inf:
        response.status = 404
        return {
        "errors": [{
        "not_found": {
        "id": id # "Replace with actual id"
        }
        }]
        }

def del_activity(table, id, activity, response):
  try:
    item = table.get_item(id=id)
    response.status = 200
    if item['activities'] != None:
      if activity in item['activities']:
        item['activities'].remove(activity)
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
    response.status = 404
    return {"errors": [{
    "not_found": {
    "id": id
    }
    }]
    }