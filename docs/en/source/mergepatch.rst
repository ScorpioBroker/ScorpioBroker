************************
Merge Patch Operation
************************

Merge Patch procedure modifies an existing NGSI-LD element by applying the set of changes found in an NGSI-LD Fragment data to the target resource. Unlike the partial update patch behaviour which replaces the complete element on the first level, e.g. a whole Attribute

This operation allows modification of an existing NGSI-LD Entity by adding new Attributes (Properties or Relationships) or modifying or deleting existing Attributes associated with an existing Entity by calling a single API.

Merge Patch Operation process:
     i) append those attributes that doesnot exist in existing data.
     ii) updated the value if attribute exist.
     iii) delete those attribute whose value is “urn:ngsi-ld:null”. 

Example for Merge Patch Operation
------------------------------------

1. Create Operation
=====================

In order to create the entity, we can hit the endpoint POST - **http://<IP Address>:<port>/ngsi-ld/v1/entities/**  with the given payload

.. code-block:: JSON

    {
        "id": "urn:ngsi-ld:smartHospital:room01",
        "type": "Room",
        "temperatureSensor": {
            "type": "Property",
            "value": "23",
            "unit": {
                "type": "Property",
                "value": "CEL"
            },
            "observedAt": "2023-06-28T13:21:00.000Z"
        },
        "humiditySensor": {
            "type": "Property",
            "value": 22
        },
        "address": {
            "type": "Property",
            "value": {
                "roomno": 2,
                "streetaddress": "luxemburg"
            }
        },
        "isPartOf": {
            "type": "Relationship",
            "object": "urn:ngsi-ld:smartHospital"
        }
    }
	
2. Merge Patch Operation
==========================

We can modify existing NGSI-LD Entity by adding new Attributes (Properties or Relationships) or modifying or deleting existing Attributes associated with an existing Entity. For that we can hit the endpoint PATCH - **http://<IP Address>:<port>/ngsi-ld/v1/entities/urn:ngsi-ld:smartHospital:room01** with the given payload

.. code-block:: JSON

    {
        "temperatureSensor": {
            "type": "Property",
            "value": 32.7,
            "observedAt": "2023-06-28T14:21:00.000Z"
        },
        "humiditySensor": {
            "type": "Property",
            "value": "urn:ngsi-ld:null"
        },
        "address": {
            "type": "Property",
            "value": {
                "roomno": 13,
                "streetaddress": "luxemburg"
            }
        }
    }
	
3. Query Operation
====================

In order to retrieve the Entity details after Merge Patch operation, make a GET request with URL **http://<IP Address>:<port>/ngsi-ld/v1/entities/urn:ngsi-ld:smartHospital:room01**

Response:

.. code-block:: JSON

    {
        "id": "urn:ngsi-ld:smartHospital:room01",
        "type": "Room",
        "temperatureSensor": {
            "type": "Property",
            "value": 32.7,
            "unit": {
                "type": "Property",
                "value": "CEL"
            },
            "observedAt": "2023-06-28T14:21:00.000Z"
        },
        "address": {
            "type": "Property",
            "value": {
                "roomno": 13,
                "streetaddress": "luxemburg"
            }
        },
        "isPartOf": {
            "type": "Relationship",
            "object": "urn:ngsi-ld:smartHospital"
        }
    }
	
Here, after retrieving the entity, we can see that the attributes "temperatureSensor" and "address" have been updated, and the attribute "humiditySensor" is removed.