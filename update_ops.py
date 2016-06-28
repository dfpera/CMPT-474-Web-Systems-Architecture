''' Add_activities function for CMPT 474 Assignment 2 '''

from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound

import json
class SetEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, set):
      return list(obj)
    if isinstance(obj, Something):
      return 'CustomSomethingRepresentation'
    return json.JSONEncoder.default(self, obj)

def add_activity(table, id, activity, response):
  try:
    item = table.get_item(id = id)
    response.status = 200
    print "type: " + type(activity)
    if item['activities'] != None:
      for x in item['activities']:
        print x
        if x == activity:
          return {"data": {
          "type": "person",
          "id": id,
          "added": []
          }
          }
      item['activities'].add(activity)
      item.save()
      return {"data": {
        "type": "person",
        "id": id,
        "added": [str(activity)]
        }
      }
    p = Item(table, data={'id': id, 'name': item['name'], 'activities': {activity}})
    p.save(overwrite=True)
    return {"data": {
    "type": "person",
    "id": id,
    "added": [str(activity)]
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