***********************
CONCISE REPRESENTATION
***********************

The concise representation is a terser, lossless form of the normalized representation, where redundant Attribute "type" members are omitted and remove the redundancy in payload so it makes easier for users to update and consume context data.
Following rules are applied for the concise representation:
 • Every Property without further sub-attributes is represented directly by the Property value only.
 • Every Property that includes further sub-attributes is represented by a value key-value pair.
 • Every GeoProperty without further sub-attributes is represented by the GeoProperty's GeoJSON representation only.
 • Every GeoProperty that includes further sub-attributes is represented by a value key-value pair.
 • Every LanguageProperty is represented by a languageMap key-value pair.
 • Every Relationship is represented by an object key-value pair.

Comparison between Normalized and Concise Representation of Entity
---------------------------------------------------------------------

1. Create Operation(Normalized Representation)
===============================================

In order to create an entity, we can hit the endpoint POST **http://<IP Address>:<port>/ngsi-ld/v1/entities/**  with the given payload.

.. code-block:: JSON

 {
     "id": "urn:ngsi-ld:Vehicle:A100",
     "type": "Vehicle",
     "brandName": {
         "type": "Property",
         "value": "Mercedes"
     },
     "street": {
         "type": "LanguageProperty",
         "languageMap": {
             "fr": "Grand Place",
             "nl": "Grote Markt"
         }
     },
     "isParked": {
         "type": "Relationship",
         "object": "urn:ngsi-ld:OffStreetParking:Downtown1",
         "observedAt": "2017-07-29T12:00:04Z",
         "providedBy": {
             "type": "Relationship",
             "object": "urn:ngsi-ld:Person:Bob"
         }
     }
 }


2. Query Operation(Normalized Representation)
==============================================

To retrieve entities with normal representation you can send an HTTP GET to **http://localhost:9090/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A100**

Response:

.. code-block:: JSON

 {
     "id": "urn:ngsi-ld:Vehicle:A100",
     "type": "Vehicle",
     "brandName": {
         "type": "Property",
         "value": "Mercedes"
     },
     "street": {
         "type": "LanguageProperty",
         "languageMap": {
             "fr": "Grand Place",
             "nl": "Grote Markt"
         }
     },
     "isParked": {
         "type": "Relationship",
         "object": "urn:ngsi-ld:OffStreetParking:Downtown1",
         "observedAt": "2017-07-29T12:00:04Z",
         "providedBy": {
             "type": "Relationship",
             "object": "urn:ngsi-ld:Person:Bob"
         }
     }
 }

3. Create Operation(Concise Representation)
===============================================

In order to create an entity with concise representation, we can hit the endpoint POST **http://<IP Address>:<port>/ngsi-ld/v1/entities/**  with the given payload.

.. code-block:: JSON

 {
     "id": " urn:ngsi-ld:Vehicle:A100",
     "type": "Vehicle",
     "brandName": "Mercedes",
     "street": {
         "languageMap": {
             "fr": "Grand Place",
             "nl": "Grote Markt"
         }
     },
     "isParked": {
         "object": "urn:ngsi-ld:OffStreetParking:Downtown1",
         "observedAt": "2017-07-29T12:00:04Z",
         "providedBy": {
             "object": "urn:ngsi-ld:Person:Bob"
         }
     }
 }

4. Query Operation(Concise Representation)
===============================================

To retrieve entities in concise representation you can send an HTTP GET to **"http://localhost:9090/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A100?option=concise"** where *"option=concise"* is used to get entity in concise representation.

Response:

.. code-block:: JSON

 {
     "id": " urn:ngsi-ld:Vehicle:A100",
     "type": "Vehicle",
     "brandName": "Mercedes",
     "street": {
         "languageMap": {
             "fr": "Grand Place",
             "nl": "Grote Markt"
         }
     },
     "isParked": {
         "object": "urn:ngsi-ld:OffStreetParking:Downtown1",
         "observedAt": "2017-07-29T12:00:04Z",
         "providedBy": {
             "object": "urn:ngsi-ld:Person:Bob"
         }
     }
 }
