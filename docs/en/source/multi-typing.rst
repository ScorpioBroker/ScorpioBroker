*************
Multi Typing
*************

NGSI-LD previous versions allows entities to have one entity type. Latest versions, allows entities to have more than one entity type at a time. We called it as multi-typing which means Multiple Entity Types are supported for any Entity.

An Entity is uniquely identified by its id, so whenever information is provided for an Entity with a given id, it is considered part of the same Entity, regardless of the Entity Type(s) specified. To avoid unexpected behaviour, Entity Types can be implicitly added by all operations that update or append attributes. There is no operation to remove Entity Types from an Entity.

The philosophy here is to assume that an Entity always had all Entity Types, but possibly not all Entity Types have previously been known in the system. The only option to remove an Entity Type is to delete the Entity and re-create it with the same id.

Type Query Examples
####################
 
- **EXAMPLE 1:** Entities of type Building or House:  
Building,House

- **EXAMPLE 2:** Entities of type Home and Vehicle: 
(Home;Vehicle)

- **EXAMPLE 3:** Entities of type (Home and Vehicle) or Motorhome:  
(Home;Vehicle),Motorhome

Example payload for Entity POST and GET with multi-typing
##########################################################
**POST API:**
::
	curl --location --request POST 'http://localhost:9090/ngsi-ld/v1/entities' \
	--header 'Content-Type: application/json' \
	--data-raw '{
		"id": "urn:ngsi-ld:test10",
		"type": [
			"Farm",
			"Building",
			"Vehicle"
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
	}'

**GET API:**
::
	curl --location --request GET 'http://localhost:9090/ngsi-ld/v1/entities?type=Vehicle,(Building;Farm)' \
	--header 'Content-Type: application/json' \
	--data-raw ''
	
	Response:
	[
		{
			"id": "urn:ngsi-ld:test7",
			"type": [
				"Vehicle",
				"Farm"
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
		},
		{
			"id": "urn:ngsi-ld:test8",
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
		},
		{
			"id": "urn:ngsi-ld:test6",
			"type": [
				"Farm",
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
		},
		{
			"id": "urn:ngsi-ld:test10",
			"type": [
				"Farm",
				"Building",
				"Vehicle"
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