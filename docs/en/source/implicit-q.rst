************************
Implicit Attribute by q
************************

According to the new specifications we can use "ngsi-ld/v1/entities?q=attrX" for querying the data which we called as Implicit Attribute by q.
 - For the GET API to add the **/ngsi-ld/v1/entities?q=attrX** and based on q to identify this request and get response data.
 - The NGSI-LD query language allows filtering entities according to the existence of attributes.
 - For example:  GET **"/entities?q=attrX"** will return only those entities that have an attribute called attrX. Regardless of the value of said attribute, if you instead want to filter on a value of an attribute: GET **"/entities?q=attrX==12"**

Example for Implicit Attribute by q
####################################

**EXAMPLE**: Give back the Entities of type Vehicle whose "brandName" attribute is "Swift"

GET API: http://localhost:9090/ngsi-ld/v1/entities?q=brandName="Swift"
	
Response:
::
	[
		{
			"id": "urn:ngsi-ld:Vehicle:A103",
			"type": "Vehicle",
			"brandName": {
				"type": "Property",
				"value": "Swift"
			},
			"@context": [
				"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
			]
		},
		{
			"id": "urn:ngsi-ld:Vehicle:A104",
			"type": "Vehicle",
			"brandName": {
				"type": "Property",
				"value": "Swift"
			},
			"@context": [
				"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
			]
		},
		{
			"id": "urn:ngsi-ld:Vehicle:A106",
			"type": "Vehicle",
			"brandName": {
				"type": "Property",
				"value": "Swift"
			},
			"@context": [
				"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
			]
		},
		{
			"id": "urn:ngsi-ld:Vehicle:A101",
			"type": "Vehicle",
			"brandName": {
				"type": "Property",
				"value": "Swift"
			},
			"isParked": {
				"type": "Relationship",
				"providedBy": {
					"type": "Relationship",
					"object": "urn:ngsi-ld:Person:Bob"
				},
				"object": "urn:ngsi-ld:OffStreetParking:Downtown1"
			},
			"speed": {
				"type": "Property",
				"value": 85
			},
			"temperature": {
				"type": "Property",
				"value": 25,
				"observedAt": "2022-03-14T01:59:26.535Z",
				"unitCode": "CEL"
			},
			"location": {
				"type": "GeoProperty",
				"value": {
					"type": "Point",
					"coordinates": [
						-8.6,
						41.6
					]
				}
			},
			"@context": [
				"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
			]
		}
	]