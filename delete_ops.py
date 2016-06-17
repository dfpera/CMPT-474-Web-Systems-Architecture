''' Delete operations for Assignment 2 of CMPT 474 '''

# Installed packages
from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound

def delete_by_id(table, id, response):
    try:
        item = table.get_item(id=id)
        response.status = 404
        p = Item(table, data={'id': id })
        p.delete()
        return {"data": {
            "type": "person",
            "id": id 
            }
        }

    except ItemNotFound as inf:
        response.status = 201 # "Created"
        return {"errors": [{
                  "not_found": {
                    "id": id
                    }
                }]
            }