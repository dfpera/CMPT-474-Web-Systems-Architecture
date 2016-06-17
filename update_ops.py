''' Add_activities function for CMPT 474 Assignment 2 '''

from boto.dynamodb2.items import Item
from boto.dynamodb2.exceptions import ItemNotFound

def add_activity(table, id, activity, response):
    try:
        item = table.get_item(id=id)
        response.status = 200
        for x in item['activities']:
            if x == activity:
                return {
                            "data": {
                                "type": "person",
                                "id": id, # "Replace with actual id"
                                "added": [] # "[] if no activity added"
                            }
                        }
        item['activities'].append(activity)
        item.parial_save()
        return {
                    "data": {
                        "type": "person",
                        "id": id, # "Replace with actual id"
                        "added": ["<activity>"] # "[] if no activity added"
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
