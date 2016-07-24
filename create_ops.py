''' Create function for CMPT 474 Assignment 2 '''

from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound

def do_create(table, id, name, response):
    try:
        item = table.get_item(id = id)
        if item["name"] != name:
            response.status = 400
            return {"errors": [{
                      "id_exists": {
                        "status": "400", # "Bad Request"
                      "title": "id already exists",
                      "detail": {"name": item["name"]}
                      }
                    }]
                }

    except ItemNotFound as inf:
        p = Item(table, data = {"id": id, "name": name, "activities": set()})
        p.save()
    
    response.status = 201 # "Created"
    
    return {"data": {
        "type": "person",
        "id": id,
        }
    }