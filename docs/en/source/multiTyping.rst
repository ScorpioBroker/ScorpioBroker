*************
Multi Typing
*************

NGSI-LD previous versions allows entities to have one entity type. Latest versions, allows entities to have more than one entity type at a time. We called it as multi-typing which means Multiple Entity Types are supported for any Entity.

An Entity is uniquely identified by its id, so whenever information is provided for an Entity with a given id, it is considered part of the same Entity, regardless of the Entity Type(s) specified. To avoid unexpected behaviour, Entity Types can be implicitly added by all operations that update or append attributes. There is no operation to remove Entity Types from an Entity.

The philosophy here is to assume that an Entity always had all Entity Types, but possibly not all Entity Types have previously been known in the system. The only option to remove an Entity Type is to delete the Entity and re-create it with the same id.

Type Query Examples
---------------------
- **EXAMPLE 1:** Entities of type Building or House:  
Building,House

- **EXAMPLE 2:** Entities of type Home and Vehicle: 
(Home%3BVehicle)

- **EXAMPLE 3:** Entities of type (Home and Vehicle) or Motorhome:  
(Home%3BVehicle),Motorhome

Example for Entity POST and GET with multi-typing
------------------------------------------------------------

1. Create Operation
=====================

In order to create an entity with multiple types, we can hit the endpoint POST **http://<IP Address>:<port>/ngsi-ld/v1/entities/**  with the given payload.

.. code-block:: JSON

 {
     "id": "urn:ngsi-ld:test2",
     "type": [
         "Vehicle",
         "Building"
     ],
     "location": {
         "type": "GeoProperty",
         "value": {
             "type": "Point",
             "coordinates": [
                 100,
                 100
             ]
         }
     },
     "name": {
         "type": "Property",
         "value": "BMW"
     },
     "speed": {
         "type": "Property",
         "value": 80
     }
 }

2. Query Operation
====================

To retrieve entity with multiple types, you can send an HTTP GET to - **http://<IP Address>:<port>/ngsi-ld/v1/entities?type={type}** and we will get all the entities having multiple types.

**EXAMPLE**: Give back the Entities of type is equals to Vehicle or Building

	GET - **http://localhost:9090/ngsi-ld/v1/entities?type=Vehicle,Building**
	
Response:

.. code-block:: JSON

 [
     {
         "id": "urn:ngsi-ld:test2",
         "type": [
             "Vehicle",
             "Building"
         ],
         "name": {
             "type": "Property",
             "value": "BMW"
         },
         "speed": {
             "type": "Property",
             "value": 80
         },
         "location": {
             "type": "GeoProperty",
             "value": {
                 "type": "Point",
                 "coordinates": [
                     100,
                     100
                 ]
             }
         }
     }
 ]