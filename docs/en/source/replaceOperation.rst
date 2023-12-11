************************
Replace Operation
************************

Previously, users did not have the support to modify an existing NGSI-LD Entity by replacing all its attributes (properties or relationships). Users had to remove the existing entity and create a new one.

Replace Operation allows the modification of an existing NGSI-LD Entity by replacing all of the Attributes (Properties or Relationships).
Replace Operation also allows the replacement of a single Attribute (Property or Relationship) within an NGSI-LD Entity.

As part this feature, we have provided following new operations:

 - Replace entire entity: PUT  **<base url>/ngsi-ld/v1/entities/<entity-id>**

 - Replace entire Attribute: PUT  **<base url>/ngsi-ld/v1/entities/<entity-id>/attrs/<attr-id>**

Example for Replace Operation
------------------------------------

1. Create Operation
=====================

In order to create the entity, we can hit the endpoint POST - **http://<IP Address>:<port>/ngsi-ld/v1/entities/**  with the given payload

.. code-block:: JSON

    {
        "id": "urn:ngsi-ld:Store:001",
        "type": "Store",
        "location": {
            "type": "GeoProperty",
            "value": {
                "type": "Point",
                "coordinates": [
                    57.5522,
                    -20.3484
                ]
            }
        },
        "address": {
            "type": "Property",
            "value": {
                "addressRegion": "Metropolis",
                "postalCode": "42420",
                "streetAddress": "Tiger Street 4",
                "addressLocality": "Cat City"
            }
        },
        "storeName": {
            "type": "Property",
            "value": "6-Stars"
        }
    }
	

2. Replace Operation
==========================

We can modify an existing NGSI-LD Entity by replacing all the Attributes (Properties or Relationships) without deleting the existing entity. For that we can hit the endpoint PUT - **http://<IP Address>:<port>/ngsi-ld/v1/entities/urn:ngsi-ld:Store:001** with the given payload

.. code-block:: JSON

    {
        "id": "urn:ngsi-ld:Store:001",
        "type": "Store",
        "address": {
            "type": "Property",
            "value": {
                "streetAddress": "Tiger Street 2",
                "addressLocality": "Cat City",
                "postalCode": "42420"
            }
        },
        "storeName": {
            "type": "Property",
            "value": "Luxury Store"
        }
    }
	
3. Query Operation
====================

In order to retrieve the Entity details after Replace operation, make a GET request with URL **http://<IP Address>:<port>/ngsi-ld/v1/entities/urn:ngsi-ld:Store:001**

Response:

.. code-block:: JSON

    {
        "id": "urn:ngsi-ld:Store:001",
        "type": "Store",
        "address": {
            "type": "Property",
            "value": {
                "streetAddress": "Tiger Street 2",
                "addressLocality": "Cat City",
                "postalCode": "42420"
            }
        },
        "storeName": {
            "type": "Property",
            "value": "Luxury Store"
        }
    }
	
Here, after retrieving the entity, we can see that all the Attributes (Properties or Relationships) are modified without deleting the existing store entity.
