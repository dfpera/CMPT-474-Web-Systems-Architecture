''' Delete operations for Assignment 2 of CMPT 474 '''

# Installed packages
from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound

def delete_by_id(table, id, response):
    try:
        item = table.get_item(id = id)
        response.status = 200
        item.delete()
        return {"data": {
            "type": "person",
            "id": id 
            }
        }

    except ItemNotFound as inf:
        response.status = 404 # "Delete"
        return {"errors": [{
                  "not_found": {
                    "id": id
                    }
                }]
            }

def delete_by_name(table, name, response):
    try:
        item = table.get_item({name : name})
        response.status = 200
        item.delete()
        return {"data": {
            "type": "person",
            "name": name
            }
        }

    except ItemNotFound as inf:
        response.status = 404 # "Delete"
        return {"errors": [{
                  "not_found": {
                    "id": id
                    }
                }]
            }