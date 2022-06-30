********************************************
New Features and Changes  in NGSI-LD v1.3.1
********************************************

Query for available Entity Types and Attributes
#################################################

In NGSI-LDv1.2.1 only the request for entities based on specific entities, entity types, and attributes was available in NGSI-LD. Now there are requests to retrieve currently available Entity Types or currently available Attributes.

- Retrieve Available Entity Types

 **/ngsi-ld/v1/types**

- Retrieve Details of Available Entity Types

 **/ngsi-ld/v1/types?details=true**

- Retrieve Available Entity Type Information

  **/ngsi-ld/v1/types/{entity type name}**

- Retrieve Available Attributes

 **/ngsi-ld/v1/attributes**

- Retrieve Details of Available Attributes

 **/ngsi-ld/v1/attributes?details=true**

- Retrieve Available Attribute Information

 **/ngsi-ld/v1/attributes/{attribute name}**



API Walkthrough
#################

1. **http://<IP Address>:<port>/ngsi-ld/v1/types**
Response:
::

 {
         "id": "urn:ngsi-ld:EntityTypeList:34534657",
         "type": "EntityTypeList",
         “typeList": [
		“Shelf",
	              "Store"
          ]
 }

2. **http://<IP Address>:<port>/ngsi-ld/v1/types?details=true**
Response:
::

 [
    {
            "id":"urn:ngsi-ld:Vehicle:A101",
            "type": "EntityType",
            "typeName": "Store",
            "attributeNames": ["storeName", "address",”location”,” contains”]
     }, 
     {
            "id":”urn:ngsi-ld:Vehicle:A102",
            "type": "EntityType",
            "typeName": "Shelf",
            "attributeNames": ["maxCapacity", "location",”isContainedIn”]     
    }
 ]

3. **http://<IP Address>:<port>/ngsi-ld/v1/types/{entity type name}**
Response:
::

 {
       "id": "http://example.org/vehicle/Vehicle",
       "type": "EntityType",
       "typeName": "Vehicle",
       "entityCount": 1,
       "attributeDetails": [
   {
         "id": "http://example.org/vehicle/brandName",
         "type": "Attribute",
         "attributeName": "brandName",
         "attributeTypes": ["Property"]
   },
   {
         "id": "http://example.org/vehicle/isParked",
         "type": "Attribute",
         "attributeName": "isParked",
         "attributeTypes": ["Relationship"]
   }
 }

4. **http://<IP Address>:<port>/ngsi-ld/v1/attributes**
Response:
::

 {
         "id": "urn:ngsi-ld:AttributeList:7896645",
         "type": “AttributeList",
         “attributeList": [
	     "contains",
	     "isContainedIn",
	     "location"]
 }

5. **http://<IP Address>:<port>/ngsi-ld/v1/attributes?details=true**
Response:
::

 [
      {
            "id": " https://uri.etsi.org/ngsi-ld/primer/contains",
            "type": "Attribute",
            "attributeName":  "contains",	
            "typeNames": ["Store"]
        },
        {
            "id": "https://uri.etsi.org/ngsi-ld/location",
            "type": "Attribute",
            "attributeName":  "location",
            "typeNames": [
                    "Store",
                    "Shelf"]
        }
 ]

6. **http://<IP Address>:<port>/ngsi-ld/v1/attributes/{attributes name}**
Response:
::

 {
            "id": "https://uri.etsi.org/ngsi-ld/location",
            "type": "Attribute",
            "attributeName":  "location",
            "attributeTypes": ["GeoProperty"],
            "typeNames": [
                    "Store",
                    "Shelf"]
            "attributeCount": 5
 }

Query Language Syntax Changes to Attribute Path
##################################################

- For better readability, we changed the attribute path representation in the Query Language.

- The attribute path is used in the query language when comparing properties/relationships of properties/relationships and elements of values with specific simple values, respectively. 

- Thus it is necessary to distinguish whether a name refers to e.g. a property of property or an element of a value.

New definition:
        property.property[value_level1.value_level2.value_level3]
Example:
      /ngsi-ld/v1/entities?q=sensor.rawdata[airquality.particulate]==40

Previous definition:
      property.property[value_level1][value_level2][value_level3]
Example:
     /ngsi-ld/v1/entities?q=sensor.rawdata[airquality][particulate]==40

Counting query results
########################
- Requests to the NGSI-LD API can return a large number of results.
- In NGSI-LD v1.3.1 it is possible to request the overall count of results, even if paging functionality is used and only a few results are returned. By setting the paging limit to 0 only the count is returned. A limit of 0 is only allowed in combination with the requesting count.
- Query operations based on HTTP GET support the query parameter count (boolean). If set to true, the response includes the special HTTP header (NGSILD-Results-Count) with the count of the overall number of available results as a value.

Operation:

URL: **http://<IPAddress>:<port>/ngsi-ld/v1/entities?type=Vehicle&count=true**

Response:

Body:
::

 [
    {
        "id": "urn:ngsi-ld:Vehicle:A202",
        "type": "Vehicle",
        "brandName": {
            "type": "Property",
            "value": "Mercedes"
        },
        "speed": [
            {
                "type": "Property",
                "datasetId": "urn:ngsi-ld:Property:speedometerA4567-speed",
                "source": {
                    "type": "Property",
                    "value": "Speedometer"
                },
                "value": 60
            },
            {
                "type": "Property",
                "datasetId": "urn:ngsi-ld:Property:gpsA4567-speed",
                "source": {
                    "type": "Property",
                    "value": "GPS"
                },
                "value": 15
            },
            {
                "type": "Property",
                "source": {
                    "type": "Property",
                    "value": "CAMERA"
                },
                "value": 12
            }
        ],
        "@context": [
            "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
        ]
    },
    {
        "id": "urn:ngsi-ld:Vehicle:A201",
        "type": "Vehicle",
        "brandName": {
            "type": "Property",
            "value": "Mercedes"
        },
        "speed": [
            {
                "type": "Property",
                "datasetId": "urn:ngsi-ld:Property:speedometerA4567-speed",
                "source": {
                    "type": "Property",
                    "value": "Speedometer"
                },
                "value": 55
            },
            {
                "type": "Property",
                "datasetId": "urn:ngsi-ld:Property:gpsA4567-speed",
                "source": {
                    "type": "Property",
                    "value": "GPS"
                },
                "value": 11
            },
            {
                "type": "Property",
                "source": {
                    "type": "Property",
                    "value": "CAMERA"
                },
                "value": 10
            }
        ],
        "@context": [
            "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
        ]
    }
 ]
 
Headers:
count:2

 URL: **http://<IP Address>:<port>/ngsi-ld/v1/entities?type=Vehicle&count=true&limit=0**

Response:

Body:
empty

Headers:
count:2

Changes in NGSI-LD v1.3.1
###########################

This change affects existing NGSI-LD data. There are some parameter changes in the below list.

.. list-table::  **Changes in NGSI-LD v1.3.1** 
   :widths: 33 33 34
   :header-rows: 1

   * - v1.2.1	
     - v1.3.1 		 
     - Impacted Area

      
   * - "description": "ngsi-ld:description"
     - "description": "http://purl.org/dc/terms/description"								
     - CSourceRegistration (optional), Subscription (optional)

   * - "end": {"@id": "ngsi-ld:end","@type": "DateTime" }
     - "endAt ": {"@id": "ngsi-ld:endAt","@type": "DateTime"}								
     - TimeInterval (CSourceRegistration, temporal case)

   * - "endTime":{"@id":"ngsild:endTime","@type": "DateTime"}
     - "endTimeAt ": {"@id": "ngsi-ld:endTimeAt","@type": "DateTime"}
     - TemporalQuery (also query parameter!)

   * - "expires": {"@id": "ngsi-ld:expires", "@type": "DateTime"}
     - "expiresAt ": {"@id": "ngsi-ld:expiresAt","@type": "DateTime"}
     - CSourceRegistration, Subscription

   * - "geometry":“ngsi-ld:geometry"
     - "geometry": "geojson:geometry"
     - GeoQuery(Subscription)

   * - ""
     - "properties": "geojson:properties"
     - New: GeoJSON Representation

   * - "properties": {"@id": "ngsi-ld:properties","@type": "@vocab"}
     - "propertyNames ": {"@id": "ngsi-ld:propertyNames","@type": "@vocab"}
     - RegistrationInfo (CSourceRegistration)

   * - "relationships": {"@id": "ngsi-ld:relationships","@type": "@vocab" }
     - "relationshipNames": {"@id": "ngsi-ld:relationshipNames","@type":"@vocab"}
     - RegistrationInfo (CSourceRegistration)

   * - ""
     - "subscriptionName": "ngsi-ld:subscriptionName"
     - Subscription

   * - "start": {"@id": "ngsi-ld:start","@type": "DateTime“}
     - "startAt": {"@id": "ngsi-ld:startAt","@type": "DateTime"}
     - TimeInterval (CSourceRegistration, temporal case)

   * - "time": {"@id": "ngsi-ld:time","@type": "DateTime“}
     - "timeAt": {"@id": "ngsi-ld:timeAt","@type": "DateTime"}
     - TemporalQuery (also query parameter!)

   * - "coordinates": "ngsi-ld:coordinates"
     - "coordinates": {"@container": "@list", "@id": "geojson:coordinates"}
     - All using GeoProperties! (e.g. Create, Update, Append, Retrieve, Query, Notify); GeoQuery(Subscription)

   * - "title": "ngsi-ld:title"
     - "title": "http://purl.org/dc/terms/title"
     - Error description

   * - "name": "ngsi-ld:name"
     - ""
     - CSourceRegistration, Subscription Replaced by “subscriptionName” and “registrationName” respectively

		
Example:

**http://<IP Address>:<port>/ ngsi-ld/v1/subscriptions/**
::

 {
  "id": "urn:ngsi-ld:Subscription:1",
  "type": "Subscription",
  "entities": [{
          "id": "urn:ngsi-ld:Vehicle:A101",
          "type": "Vehicle"
        }],
  "watchedAttributes": ["brandName"],
        "q":"brandName!=Mercedes",  "subscriptionName":"SubscriptionName",       
  "description":"ngsi-ld:description",  "expiresAt":"2021-07-29T12:00:04Z",
  "notification": {
   "attributes": ["brandName"],
   "format": "keyValues",
   "endpoint": {
    "uri": "mqtt://localhost:1883/notify",
    "accept": "application/json",
     "notifierinfo": {
       "version" : "mqtt5.0",
       "qos" : 0
     }
   }
  }
 }
