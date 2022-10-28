************
POST QUERIES
************

The reason to provide a way to query NGSI_LD entities via POST, is that using GET has the following issue:

1. We end up assembling very long URLs,due to URI parameters for 'id', 'attrs', etc, being included in the URL.
2. By using POST, there is no need of long URLs.

So, the difference lies in that instead of passing the inputs as URI parameters, in a POST Query, we pass all the query items in the payload body.

DESIGN
######

We created a new **/entityOperations/query** endpoint in the API
As we can see this new POST query operation has just one single query as input.

 - It is not allowed to send multiple queries in a single request as it is not considered as a "BATCH Operation"


Data Type of the Request Payload Body
#####################################

.. list-table::  **Data Types**
   :widths: 20 20 45 15
   :header-rows: 1

   * - Name
     - Data Type	 
     - Restrictions
     - Cardinality
	 
	 
   * - type
     - string
     - It shall be equal to "Query"
     - 1
	 
   * - entities
     - EntityInfo[]
     - Empty array (0 length) is not allowed.
     - 0.1
	 
   * - attrs
     - string[]
     - Attribute Name as short‑hand string. Empty array (0 length) is not allowed.
     - 0.1
	 
   * - q
     - string
     - A valid query string
     - 0.1
	 
   * - geoQ
     - GeoQuery
     - A valid GeoJSON geometry
     - 0.1
	 
   * - csf
     - string
     - A valid query string
     - 0.1
	 
   * - temporalQ
     - TemporalQuery
     - A valid temporal operation
     - 0.1
	 
	 
POST Query Example
##################

Here is an example of Request URL and Payload data that shows how we can pass the query in the Payload.

POST  **http://localhost:9090/ngsi-ld/v1/entityOperations/query** :
::
 {
     "type": "Query",
     "entities": [
         {
             "id": "smartcity:houses:house2",
             "type": "House"
         }
     ]
 }
 
In this Post Query we haven't defined any attributes so, all Attributes will be retrieved and our result looks like this.
::
 [ {
   "id" : "smartcity:houses:house2",
   "type" : "House",
   "entrance" : {
     "type" : "GeoProperty",
     "value" : {
       "type" : "Point",
       "coordinates" : [ -8.50000005, 41.2 ]
     }
   },
   "hasRoom" : [ {
     "type" : "Relationship",
     "datasetId" : "urn:room:0816",
     "object" : "house2:smartrooms:room1"
   }, {
     "type" : "Relationship",
     "datasetId" : "urn:room:0815",
     "object" : "house2:smartrooms:room2"
   } ],
   "location" : {
     "type" : "GeoProperty",
     "value" : {
       "type" : "Polygon",
       "coordinates" : [ [ [ -8.5, 41.2 ], [ -8.5000001, 41.2 ], [ -8.5000001, 41.2000001 ], [ -8.5, 41.2000001 ], [ -8.5, 41.2 ] ] ]
     }
   }
 } ]
