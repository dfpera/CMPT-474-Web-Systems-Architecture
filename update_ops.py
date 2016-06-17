''' Add_activities function for CMPT 474 Assignment 2 '''

from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound

def add_activity(table, id, activity, response):
  try:
    item = table.get_item(id=id)
    response.status = 200
    if item['activities'] != None:
      for x in item['activities']:
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
      "added": [activity]
      }
      }

    p = Item(table, data={'id': id, 'name': item['name'], 'activities': {activity}})
    p.save(overwrite=True)
    return {"data": {
    "type": "person",
    "id": id,
    "added": []
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