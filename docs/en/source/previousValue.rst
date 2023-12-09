************************
Previous Value Support
************************

**Previous behaviour:** notification will be triggered only when entity is created and updated. There is no option for user to get previous value in notification instead if user need to fetch the previous value of attributes, user can check previous value from historical data which is time consuming.

**New behaviour:** we will be providing attribute notification Trigger string array to configure the notification cases. With the implementation of Previous Value Support feature user can get previous value of attributes in the notification itself. It will support the following values: *entityUpdated, entityDeleted, attributeUpdated, attributeDeleted*

Merge Patch Operation process:
    - We are introducing new property previousValue to be send in the notification if subscriber enable **showChanges** property. 

+--------------+-----------------------+------------------------------+--------------------------+-------------------------------------------------------------+
| Name         | Data Type             | Restrictions                 | Cardinality              | Description                                                 |
+==============+=======================+==============================+==========================+=============================================================+
| showChanges  | boolean               | false by default             | 0..1                     | If true, the previous value (previousValue) of Properties   |    
|              |                       |                              |                          | or languageMap (previousLanguageMap) of Language Properties |
|              |                       |                              |                          | or object (previousObject) of Relationships is provided     |
|              |                       |                              |                          | in addition to the current one. This requires that it       |
|              |                       |                              |                          | exists, i.e. in case of modifications and deletions         |
|              |                       |                              |                          | , but not in the case of creations. showChanges             |
|              |                       |                              |                          | cannot be true in case format is "keyValues".               | 
+--------------+-----------------------+------------------------------+--------------------------+-------------------------------------------------------------+

Example for Previous Value Support
------------------------------------

1. Create Subscription Operation
==================================

In order to create subscription, we can hit the endpoint POST - **http://<IP Address>:<port>/ngsi-ld/v1/subscriptions/**  with the given payload

.. code-block:: JSON

    {
        "id": "urn:ngsi-ld:Subscription:001",
        "type": "Subscription",
        "entities": [
            {
                "type": "Farm"
            }
        ],
        "notification": {
            "showChanges": true,
            "endpoint": {
                "uri": "http://localhost:8080",
                "accept": "application/json"
            }
        }
    }
	
2. Create Operation
======================

In order to create an entity, we can hit the endpoint POST - **http://<IP Address>:<port>/ngsi-ld/v1/entities/**  with the given payload

.. code-block:: JSON

    {
        "id": "urn:ngsi-ld:Smartfarm:001",
        "type": "Farm",
        "temperatureSensor": {
            "type": "Property",
            "value": "22.7",
            "unit": {
                "type": "Property",
                "value": "CEL"
            }
        },
        "humiditySensor": {
            "type": "Property",
            "value": 40
        }
    }
	
- **Notification for Entity Creation**

.. code-block:: JSON

    {
        "body": {
            "id": "notification:-7635173128633396166",
            "type": "Notification",
            "subscriptionId": "urn:ngsi-ld:Subscription:001",
            "notifiedAt": "2023-06-27T09:44:22.331000Z",
            "data": [
                {
                    "id": "urn:ngsi-ld:Smartfarm:001",
                    "type": "Farm",
                    "humiditySensor": {
                        "type": "Property",
                        "value": 40
                    },
                    "temperatureSensor": {
                        "type": "Property",
                        "unit": {
                            "type": "Property",
                            "value": "CEL"
                        },
                        "value": "22.7"
                    }
                }
            ]
        }
    }

3. Update Operation
======================

To retrieve the previous value of a particular instance after updating it, we can make a PATCH request with URL **http://<IP Address>:<port>/ngsi-ld/v1/entities/urn:ngsi-ld:Smartfarm:001**

Response:

.. code-block:: JSON

    {
        "id": "urn:ngsi-ld:Smartfarm:001",
        "type": "Farm",
        "temperatureSensor": {
            "type": "Property",
            "value": "30.28",
            "unit": {
                "type": "Property",
                "value": "CEL"
            }
        },
        "humiditySensor": {
            "type": "Property",
            "value": 56
        }
    }
	
- **Notification for Updated Entity**

.. code-block:: JSON

    {
        "body": {
            "id": "notification:-5500123694573792689",
            "type": "Notification",
            "subscriptionId": "urn:ngsi-ld:Subscription:001",
            "notifiedAt": "2023-06-27T09:50:03.518000Z",
            "data": [
                {
                    "id": "urn:ngsi-ld:Smartfarm:001",
                    "type": "Farm",
                    "humiditySensor": {
                        "type": "Property",
                        "previousValue": 40,
                        "value": 56
                    },
                    "temperatureSensor": {
                        "type": "Property",
                        "previousValue": "22.7",
                        "value": "30.28"
                    }
                }
            ]
        }
    }

Here, the previous value support feature enables the user to view the previous values of humiditySensor and temperatureSensor via notifications. The example represents the values being retrieved in JSON format.