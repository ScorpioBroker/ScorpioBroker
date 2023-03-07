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

Concise Representation Example
###############################

1. Comparison between Normalized and Concise Representation
------------------------------------------------------------

- **Normalized Representation**

GET API: http://localhost:9090/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A100

Response:
::	
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
				 "nl": "Grote Markt
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

- **Concise Representation**

GET API: http://localhost:9090/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A100?option=concise

Response:
::
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

2. Location based example with concise
---------------------------------------

The concise GeoJSON representation of a single Entity is defined as a single GeoJSON Feature object as follows:

 - "type": shall be a supported GeoJSON geometry.
 - "coordinates": shall be present.

1. **Nomalized Representation for GeoJSON**

GET API: http://localhost:9090/ngsi-ld/v1/entities/smartcity:building:building1

Response:
::
	{
	    "id": "smartcity:building:building1",
	    "type": "Building",
	    "location": {
		"type": "GeoProperty",
		"value": {
		    "type": "Point",
		    "coordinates": [
			-8.50000005,
			41.2
		    ]
		}
	    },
	    "@context": [
		"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
	    ]
	}

2. **Concise Representation for GeoJSON**

GET API: http://localhost:9090/ngsi-ld/v1/entities/smartcity:building:building1?option=concise

Response:
::
	{
	    "id": "smartcity:building:building1",
	    "type": "Building",
	    "location": {
		"type": "Point",
		"coordinates": [
		    -8.50000005,
		    41.2
		]
	    },
	    "@context": [
		"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.6.jsonld"
	    ]
	}
