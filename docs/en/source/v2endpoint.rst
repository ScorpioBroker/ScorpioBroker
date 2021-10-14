***********************************
Scorpio NGSI V2 to NGSI-LD endpoint
***********************************

Scorpio provides an endpoint for NGSI V2 context producers. This endpoint will attempt to transform the provided V2 entities. 
After the transformation process scorpio will use NGSI-LD upsert to store or update the LD versions of the entities. This means this endpoint can be used to create entities as well as update or append new attributes to existing entities.

Translation Logic
#################
Since V2 and LD don't have 100% compatible concepts certain assumptions had to be made to allow this automated transformation.

 - ID:
If the provided ID is not a URI it will be prefixed with urn:v2told: making it a URI.
Scoprio will provide the correct resource adresses for each entity in the result of the operation
 - Type:
Will be used as is. @Context provided in a header will be respected. See HTTP header handling.
 - Attributes:
If attributes are not identified as Relationship or Geoproperty they will be translated as Property. This means a V2 property will be packaged into the value field of an LD property.
e.g.:
::
	"name": {
        "type": "Text",
        "value": "Checkpoint Markt"
    }
will become 
::
	"name": {
		"type": "Property",
		"value": {
			"type": "Text",
			"value": "Checkpoint Markt"
		}
    }
If you provide the HTTP parameter shortenprops Scorpio will remove the old V2 type entry and set the value directly as value of the new Property.
::
	"name": {
        "type": "Property",
        "value": "Checkpoint Markt"
    }
 - Metadata:
V2 has a metadata field which can be used on all attributes. In LD all attributes can directly have LD sub attributes. (So Property of Property, Relationship of Relationship, Property of Relationship, etc.) This means the metadata field will be removed in the LD entity and metadata will be added directly as sub attribute. 
E.g.
::
	"address": {
        "type": "PostalAddress",
        "value": {
            "streetAddress": "Friedrichstraße 50",
            "addressRegion": "Berlin",
            "addressLocality": "Kreuzberg",
            "postalCode": "10969"
        },
        "metadata": {
            "verified": {
                "value": true,
                "type": "Boolean"
            }
        }
    }
assuming we shortend properties will be translated as 
::
	"address": {
        "type": "Property",
        "verified": {
            "type": "Property",
            "value": true
        },
        "value": {
            "addressLocality": "Kreuzberg",
            "addressRegion": "Berlin",
            "postalCode": "10969",
            "streetAddress": "Friedrichstraße 50"
        }
    }
 - Relationships:
V2 has no official distinction between different types of attribute. This is done through naming conventions. Currently Scorpio will attempt to translate any attribute that starts with "ref" or has the type Relationship as a Relationship. If this fails due to a none expected value or something similar Scorpio will fall back to translate it as Property. Similar to the ID field the LD requirement for a URI as value has to be satisfied. 
e.g.
::
	"refStore": {
        "type": "Relationship",
        "value": "urn:ngsi-ld:Store:001"
    }
will become 
::
	"refStore": {
        "type": "Relationship",
        "object": "urn:ngsi-ld:Store:001"
    }
 - Multi value relationships:
In V2 multi value relationships are normally done bei providing the value as an array. LD has a different approach here. In order to seperate different values a mandatory user provided field named datasetId is required. This endpoint will autogenerate them on the basis of a prefix created from the entity ID, the attribute name and the position of the relationship in the array. 
e.g.
::
	"refMulti": {
        "type": "Relationship",
        "value": ["urn:ngsi-ld:Store:001", "urn:ngsi-ld:Store:002", "urn:ngsi-ld:Store:003"]
    }
will become
::
	"refMulti": [
        {
            "type": "Relationship",
            "datasetId": "urn:v2told:urn:ngsi-ld:Store:004:refMulti:1",
            "object": "urn:ngsi-ld:Store:001"
        },
        {
            "type": "Relationship",
            "datasetId": "urn:v2told:urn:ngsi-ld:Store:004:refMulti:2",
            "object": "urn:ngsi-ld:Store:002"
        },
        {
            "type": "Relationship",
            "datasetId": "urn:v2told:urn:ngsi-ld:Store:004:refMulti:3",
            "object": "urn:ngsi-ld:Store:003"
        }
    ]
 - Geoproperties:
If the type of the attribute is geo:json or the attributes value contains a coordinates and a type field this endpoint will treat the attribute as a Geoproperty.
e.g.:
::
	"location": {
        "type": "geo:json",
        "value": {
             "type": "Point",
             "coordinates": [13.3903, 52.5075]
        }
    }
will become
::
	"location": {
        "type": "GeoProperty",
        "value": {
            "type": "Point",
            "coordinates": [
                13.3903,
                52.5075
            ]
        }
    }
observedAt generation:
Via the HTTP paramater observedat you can provide a list of valid metadata entries which should be used to generate an observedAt entry in an attribute. The value of the attribute has to be compliant with the NGSI-LD specifications.