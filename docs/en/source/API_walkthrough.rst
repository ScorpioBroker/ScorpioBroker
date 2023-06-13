************
Introduction
************

This walkthrough adopts a practical approach that we hope will help our readers to get familiar with NGSI-LD in general and the Scorpio Broker in particular - and have some fun in the process :).

The walkthrough is based on the NGSI-LD Specification, that can be found in here [https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.02.02_60/gs_CIM009v010202p.pdf]. --> will become gs_CIM009v010301p.pdf soon ...
You should also have a look at the NGSI-LD implementation notes. --> once they are available
To get familiar with NGSI-LD, you may also have a look at the NGSI-LD Primer [https://www.etsi.org/deliver/etsi_gr/CIM/001_099/008/01.01.01_60/gr_CIM008v010101p.pdf] that is targeted at developers.

The main section is about context management. It describes the basic context broker functionality for context management (information about entities, such as the temperature of a car). Context source management (information not about the entities themselves, but about the sources  that can provide the information in a distributed system setup) is also described as part of this document.

It is recommended to get familiar with the theoretical concepts on which the NGSI-LD model is based before starting. E.g. entities, properties, relationships etc. Have a look at the FIWARE documentation about this, e.g. this public presentation. [... find suitable presentation]


Starting the Scorpio Broker for the tutorials
#############################################

In order to start the broker we recommend to use docker-compose. Get the docker-compose file from the github repo of Scorpio.
::

	curl https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/development/docker-compose-aaio.yml 

and start the container with 
::

	sudo docker-compose -f docker-compose-aaio.yml up

You can also start the broker without docker. For further instructions please refer to the readme https://github.com/ScorpioBroker/ScorpioBroker/blob/development/README.md 


Issuing commands to the broker
##############################

To issue requests to the broker, you can use the curl command line tool. curl is chosen because it is almost ubiquitous in any GNU/Linux system and simplifies including examples in this document that can easily be copied and pasted. Of course, it is not mandatory to use it, you can use any REST client tool instead (e.g. RESTClient). Indeed, in a real case, you will probably interact with the Scorpio Broker using a programming language library implementing the REST client part of your application.

The basic patterns for all the curl examples in this document are the following:

For POST:
curl localhost:9090/ngsi-ld/v1/<ngsi-ld-resource-path> -s -S [headers]' -d @- <<EOF
[payload]
EOF
For PUT:
curl localhost:9090/ngsi-ld/v1/<ngsi-ld-resource-path> -s -S [headers] -X PUT -d @- <<EOF
[payload]
EOF
For PATCH:
curl localhost:9090/ngsi-ld/v1/<ngsi-ld-resource-path> -s -S [headers] -X PATCH -d @- <<EOF
[payload]
EOF
For GET:
curl localhost:9090/ngsi-ld/v1/<ngsi-ld-resource-path> -s -S [headers]
For DELETE:
curl localhost:9090/ngsi-ld/v1/<ngsi-ld-resource-path> -s -S [headers] -X DELETE
Regarding [headers] you have to include the following ones:

Accept header to specify the payload format in which you want to receive the response. You should explicitly specify JSON or JSON-LD.
curl ... -H 'Accept: application/json' ... or curl ... -H 'Accept: application/ld-json' depending on whether you want to
receive the JSON-LD @context in a link header or in the body of the response (JSON-LD and the use of @context is described in the
following section).

If using payload in the request (i.e. POST, PUT or PATCH), you have to supply the Context-Type HTTP header to specify the format (JSON or JSON-LD).
curl ... -H 'Content-Type: application/json' ... or -H 'Content-Type: application/ld+json'

In case the JSON-LD @context is not provided as part of the request body, it has to be provided as a link header, e.g.
curl ... -H 'Link: <https://uri.etsi.org/ngsi-ld/primer/store-context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json" where the @context has to be retrievable from the first URI, i.e. in this example: https://uri.etsi.org/ngsi-ld/primer/store-context.jsonld

Some additional remarks:

Most of the time we are using multi-line shell commands to provide the input to curl, using EOF to mark the beginning and the end of the multi-line block (here-documents). In some cases (GET and DELETE) we omit -d @- as no payload is used.

In the examples, it is assumed that the broker is listening on port 9090. Adjust this in the curl command line if you are using a different port.

In order to pretty-print JSON in responses, you can use Python with msjon.tool (examples along with tutorial are using this style):

(curl ... | python -mjson.tool) <<EOF
...
EOF

Check that curl is installed in your system using:
::

	which curl


NGSI-LD data in 3 sentences
###########################

NGSI-LD is based on JSON-LD. 
Your toplevel entries are NGSI-LD Entities.
Entities can have Properties and Relationships and Properties and Relationships can themselves also have Properties and Relationships (meta information).
All keys in the JSON-LD document must be URIs, but there is a way to shorten it.

@context
########

NGSI-LD builds upon JSON-LD. Coming from JSON-LD there is the concecpt of a mandatory @context entry which is used to 'translate' between expanded full URIs and a compacted short form of the URI. e.g. 
"Property": "https://uri.etsi.org/ngsi-ld/Property".
@context entries can also be linked in via a URL in a JSON array. You can also mix this up, so this is perfectly fine.
::

	{
		"@context": [{
			"myshortname": "urn:mylongname"
		},
		"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
		]
	}

NGSI-LD has a core context made available at https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld. Even though it is highly recommended to always provide a full entry of all used @context entries, Scorpio and other NGSI-LD brokers will inject the core context on any entry where it is missing.

application/json and application/ld+json
########################################

You can provide and receive data in two different ways. The main difference between application/json and application/ld+json is where you provide or receive the mandatory @context entry. If you set the accept header or the content-type header to application/ld+json the @context entry is embedded in the JSON document as a root level entry. If it is set to application/json the @context has to be provided in a link in the header entry Link like this.
<https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

Context Management
##################

To show the use of @context, most examples in this tutorial will be done as application/ld+json having the @context entries in the body of the payload.
At the end of this section, you will have the basic knowledge to create applications (both context producers and consumers) using the Scorpio Broker with context management operations.

***************
Entity creation
***************

Assuming a fresh start we have an empty Scorpio Broker.
First, we are going to create house2:smartrooms:room1. Let's assume that at entity creation time, temperature is 23 ?C and it is part of smartcity:houses:house2.
::

	curl localhost:9090/ngsi-ld/v1/entities -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
		{
	  "id": "house2:smartrooms:room1",
	  "type": "Room",
	  "temperature": {
		"value": 23,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor0815"
		 }
	   },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house2"
	  },
	  "@context": [{"Room": "urn:mytypes:room", "temperature": "myuniqueuri:temperature", "isPartOf": "myuniqueuri:isPartOf"},"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
	}
	EOF

Apart from the id and type fields (that define the ID and type of the entity), the payload contains a set of attributes. As you can see, there are two types of attributes. Properties and Relationships. Properties directly provide a value of an attribute. Additionally there is an optional parameter unitCode which can be used to better describe the value using unit codes described in UN/CEFACT Common Codes for Units of Measurement. 
UnitCodes should be seen as an aditional metadata provided by the producer. They are not restrictive. There is no validation on the value field.

Relationships always point to another Entity encoded as the object of a relationship. They are used to describe the relations between various entities. Properties and Relationship can themselves have Relationships, enabling the representation of meta information. As you can see we also added a Relationship to the temperature Property pointing to an Entity describing the sensor from which this information has been received.

Upon receipt of this request, Scorpio creates the entity in its internal database and takes care of any further handling required because of the creation, e.g. subscription handling or creating historical entries. Once the request is validated Scorpio responds with a 201 Created HTTP code.

Next, let's create house2:smartrooms:room2 in a similar way.
::

	curl localhost:9090/ngsi-ld/v1/entities -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "house2:smartrooms:room2",
	  "type": "Room",
	  "temperature": {
		"value": 21,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor4711"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house2"
	  },
	  "@context": [{"Room": "urn:mytypes:room", "temperature": "myuniqueuri:temperature", "isPartOf": "myuniqueuri:isPartOf"},"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
	}
	EOF

Now to complete this setup we are creating an Entity describing our house with the id smartcity:houses:house2.
::

	curl localhost:9090/ngsi-ld/v1/entities -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
		"id": "smartcity:houses:house2",
		"type": "House",
		"hasRoom": [{
			"type": "Relationship",
			"object": "house2:smartrooms:room1",
			"datasetId": "somethingunique1"
		},
		{
			"type": "Relationship",
			"object": "house2:smartrooms:room2",
			"datasetId": "somethingunique2"
		}],
		"location": {
			"type": "GeoProperty",
			"value": {
				"type": "Polygon",
				"coordinates": [[[-8.5, 41.2], [-8.5000001, 41.2], [-8.5000001, 41.2000001], [-8.5, 41.2000001], [-8.5, 41.2]]]
			}
		},
		"entrance": {
			"type": "GeoProperty",
			"value": {
				"type": "Point",
				"coordinates": [-8.50000005, 41.2]
			}
		},
		"@context": [{"House": "urn:mytypes:house", "hasRoom": "myuniqueuri:hasRoom"},"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
	}
	EOF

Even though you can of course model this differently, for this scenario we model the relationships of houses with rooms with a hasRoom entry as a multi-relationship. To uniquely identify the entries they have a datasetId, which is also used when updating this specific relationship. There can be at most one relationship instance per relationship without a datasetId, which is considered to be the "default" instance. In the case of properties, multi-properties are represented in the same way. 
Additionally we are using a third type of attribute here the GeoProperty. GeoProperty values are  GeoJSON values, allowing the description of various shapes and forms using longitude and latitude. Here we add to entries location, describing the outline of the house, and entrance, pointing to the entrance door.

As you might have seen, we haven't provided an @context entry for 'entrance' and unlike 'location' it is not part of the core context. This will result in Scorpio storing the entry using a default prefix defined in the core context. The result in this case would be "https://uri.etsi.org/ngsi-ld/default-context/entrance".

Apart from simple values corresponding to JSON datatypes (i.e. numbers, strings, booleans, etc.) for attribute values, complex structures or custom metadata can be used. 

*****************************
Querying & receiving entities
*****************************

Taking the role of a consumer application, we want to access the context information stored in Scorpio. 
NGSI-LD has two ways to get entities. You can either receive a specific entity using a GET /ngsi-ld/v1/entities/{id} request. The alternative is to query for a specific set of entities using the NGSI-LD query language.

If we want to just get the house in our example we would do a GET request like this.
::

	curl localhost:9090/ngsi-ld/v1/entities/smartcity%3Ahouses%3Ahouse2 -s -S -H 'Accept: application/ld+json' 

Mind the url encoding here, i.e. ':' gets replaced by %3A. For consistency you should always encode your URLs. 

Since we didn't provide our own @context in this request, only the parts of the core context will be replaced in the reply.
::

	{
		"id": "smartcity:houses:house2",
		"type": "urn:mytypes:house",
		"myuniqueuri:hasRoom": [{
			"type": "Relationship",
			"object": "house2:smartrooms:room1",
			"datasetId": "somethingunique1"
		},
		{
			"type": "Relationship",
			"object": "house2:smartrooms:room2",
			"datasetId": "somethingunique2"
		}],
		"location": {
			"type": "GeoProperty",
			"value": {
				"type": "Polygon",
				"coordinates": [[[-8.5, 41.2], [-8.5000001, 41.2], [-8.5000001, 41.2000001], [-8.5, 41.2000001], [-8.5, 41.2]]]
			}
		},
		"entrance": {
			"type": "GeoProperty",
			"value": {
				"type": "Point",
				"coordinates": [-8.50000005, 41.2]
			}
		}
		"@context": ["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
	}

As you can see entrance was compacted properly since it is was prefixed from the default context specified in the core context.

Assuming we are hosting our own @context file on a webserver, we can provide it via the 'Link' header.
For convience we are using pastebin in this example 
Our context looks like this.
::

	{
		"@context": [{
			"House": "urn:mytypes:house",
			"hasRoom": "myuniqueuri:hasRoom",
			"Room": "urn:mytypes:room",
			"temperature": "myuniqueuri:temperature",
			"isPartOf": "myuniqueuri:isPartOf"
		}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
	}

We repeat this call providing our @context via the 'Link' like this 
::

	curl localhost:9090/ngsi-ld/v1/entities/smartcity%3Ahouses%3Ahouse2 -s -S -H 'Accept: application/ld+json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"' 

The reply now looks like this.
::

	{
		"id": "smartcity:houses:house2",
		"type": "House",
		"hasRoom": [{
			"type": "Relationship",
			"object": "house2:smartrooms:room1",
			"datasetId": "somethingunique1"
		},
		{
			"type": "Relationship",
			"object": "house2:smartrooms:room2",
			"datasetId": "somethingunique2"
		}],
		"location": {
			"type": "GeoProperty",
			"value": {
				"type": "Polygon",
				"coordinates": [[[-8.5, 41.2], [-8.5000001, 41.2], [-8.5000001, 41.2000001], [-8.5, 41.2000001], [-8.5, 41.2]]]
			}
		},
		"entrance": {
			"type": "GeoProperty",
			"value": {
				"type": "Point",
				"coordinates": [-8.50000005, 41.2]
			}
		},
		"@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	
Since we provide the core context in our own @context it is not added to the result.
From here on we will use the custom @context so we can use the short names in all of our requests.

You can also request an entity with a single specified attribute, using the attrs parameter. For example, to get only the location:
::

	curl localhost:9090/ngsi-ld/v1/entities/smartcity%3Ahouses%3Ahouse2/?attrs=location -s -S -H 'Accept: application/ld+json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"' 

Response:
::

	{
		"id": "smartcity:houses:house2",
		"type": "House",
		"location": {
			"type": "GeoProperty",
			"value": {
				"type": "Polygon",
				"coordinates": [[[-8.5, 41.2], [-8.5000001, 41.2], [-8.5000001, 41.2000001], [-8.5, 41.2000001], [-8.5, 41.2]]]
			}
		},
		"@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}

Query
#####

The second way to retrieve information is the NGSI-LD query. 
For this example we first add a new Room which belongs to another house.
::

	curl localhost:9090/ngsi-ld/v1/entities -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "house99:smartrooms:room42",
	  "type": "Room",
	  "temperature": {
		"value": 21,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house99:sensor36"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house99"
	  },
	  "@context": [{"Room": "urn:mytypes:room", "temperature": "myuniqueuri:temperature", "isPartOf": "myuniqueuri:isPartOf"},"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
	}
	EOF

Let's assume we want to retrieve all the rooms in Scorpio. To do that we do a GET request like this
::

	curl localhost:9090/ngsi-ld/v1/entities/?type=Room -s -S -H 'Accept: application/json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'

Note that this request has the accept header application/json, i.e. the link to the @context is returned in a link header.
The result is
::

	[
	{
	  "id": "house2:smartrooms:room1",
	  "type": "Room",
	  "temperature": {
		"value": 23,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor0815"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house2"
	  }
	  
	},
	{
	  "id": "house2:smartrooms:room2",
	  "type": "Room",
	  "temperature": {
		"value": 21,
		"unitCode": "CEL",
		"type": "Property"
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor4711"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house2"
	  }
	},
	{
	  "id": "house99:smartrooms:room42",
	  "type": "Room",
	  "temperature": {
		"value": 21,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house99:sensor36"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house99"
	  }
	}
	]

Filtering
#########

NGSI-LD provides a lot of ways to filter Entities from query results (and subscription notifications respectively). 
Since we are only interested in our smartcity:houses:house2, we are using the 'q' filter on the Relatioship isPartOf. 
(URL encoding "smartcity:houses:house2" becomes %22smartcity%3Ahouses%3Ahouse2%22)
::

	curl localhost:9090/ngsi-ld/v1/entities/?type=Room\&q=isPartOf==%22smartcity%3Ahouses%3Ahouse2%22 -s -S -H 'Accept: application/json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'

The results now looks like this.
::
	
	[
	{
	  "id": "house2:smartrooms:room1",
	  "type": "Room",
	  "temperature": {
		"value": 23,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor0815"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house2"
	  }
	  
	},
	{
	  "id": "house2:smartrooms:room2",
	  "type": "Room",
	  "temperature": {
		"value": 21,
		"unitCode": "CEL",
		"type": "Property"
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor4711"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house2"
	  }
	}
	]

Now an alternative way to get the same result would be using the idPattern parameter, which allows you to use regular expressions. This is possible in this case since we structured our IDs for the rooms.
::

	curl localhost:9090/ngsi-ld/v1/entities/?type=Room\&idPattern=house2%3Asmartrooms%3Aroom.%2A -s -S -H 'Accept: application/json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
	(house2%3Asmartrooms%3Aroom.%2A == house2:smartrooms:room.*)

Limit the attributes
####################

Additionally we now want to limit the result to only give us the temperature. This is done by using the attrs parameter. Attrs takes a comma seperated list. In our case since it's only one entry it looks like this.
::

	curl localhost:9090/ngsi-ld/v1/entities/?type=Room&q=isPartOf==%22smartcity%3Ahouses%3Ahouse2%22\&attrs=temperature -s -S -H 'Accept: application/json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'

::

	[
	{
	  "id": "house2:smartrooms:room1",
	  "type": "Room",
	  "temperature": {
		"value": 23,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor0815"
		}
	  }
	  
	},
	{
	  "id": "house2:smartrooms:room2",
	  "type": "Room",
	  "temperature": {
		"value": 21,
		"unitCode": "CEL",
		"type": "Property"
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor4711"
		}
	  }
	}
	]

KeyValues results
#################

Now assuming we want to limit the payload of the request even more since we are really only interested in the value of temperature and don't care about any meta information. This can be done using the keyValues option. KeyValues will return a condenced version of the Entity providing only top level attribute and their respective value or object.
::

	curl localhost:9090/ngsi-ld/v1/entities/?type=Room\&q=isPartOf==%22smartcity%3Ahouses%3Ahouse2%22\&attrs=temperature\&options=keyValues -s -S -H 'Accept: application/json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'

Response:
::

	[
	{
	  "id": "house2:smartrooms:room1",
	  "type": "Room",
	  "temperature": 23
	},
	{
	  "id": "house2:smartrooms:room2",
	  "type": "Room",
	  "temperature": 21
	}
	]

*******************************************
Updating an entity & appending to an entity
*******************************************

NGSI-LD allows you to update entities (overwrite the current entry) but also to just append new attributes. 
Additonally you can of course just update a specific attribute.
Taking the role of the Context Producer for the temperature for house2:smartrooms:room1 we will cover 5 scenarios.
1. Updating the entire entity to push new values.
2. Appending a new Property providing the humidity from the room.
3. Partially updating the value of the temperature.
4. Appending a new multi value entry to temperature providing the info in degree Kelvin 
5. Updating the specific multi value entries for temperature and Fahrenheit.

Update Entity
#############

You can basically update every part of an entity with two exceptions. The type and the id are immutable. An update in NGSI-LD overwrites the existing entry. This means if you update an entity with a payload which does not contain a currently existing attribute it will be removed.
To update our room1 we will do an HTTP POST like this.
::

	curl localhost:9090/ngsi-ld/v1/entities/house2%3Asmartrooms%3Aroom1 -s -S -H 'Content-Type: application/json' -H 'Link: https://pastebin.com/raw/Mgxv2ykn' -d @- <<EOF
	{
		"temperature": {
		"value": 25,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor0815"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house2"
	  }
	}
	EOF
	
Now this is a bit much payload to update one value and there is a risk that you might accidently delete something and we would only recommend this entity update if you really want to update a bigger part of an entity.

Partial update attribute
########################

To take care of a single attribute update NGSI-LD provides a partial update. This is done by a POST on /entities/<entityId>/attrs/<attributeName>
In order to update the temperature we do a POST like this 
::

	curl localhost:9090/ngsi-ld/v1/entities/house2%3Asmartrooms%3Aroom1/attrs/temperature -s -S -H 'Content-Type: application/json' -H 'Link: https://pastebin.com/raw/Mgxv2ykn' -d @- <<EOF
	{
		"value": 26,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor0815"
		}
	}
	EOF
	
Append attribute
################

In order to append a new attribute to an entity you execute an HTTP PATCH command on /entities/<entityId>/attrs/ with the new attribute as payload.
Append in NGSI-LD by default will overwrite an existing entry. If this is not desired you can add the option parameter with noOverwrite to the URL like this /entities/<entityId>/attrs?options=noOverwrite. Now if we want to add an additional entry for the humidity in room1 we do an HTTP PATCH like this. 
::

	curl localhost:9090/ngsi-ld/v1/entities/house2%3Asmartrooms%3Aroom1/attrs -s -S -X PATCH -H 'Content-Type: application/json' -H 'Link: https://pastebin.com/raw/Mgxv2ykn' -d @- <<EOF
	{
		"humidity": {
		"value": 34,
		"unitCode": "PER",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor2222"
		}
	  }
	}
	
Add a multivalue attribute
##########################

NGSI-LD also allows us to add new multi value entries. We will do this by adding a unique datesetId. If a datasetId is provided in an append it will only affect the entry with the given datasetId. Adding the temperature in Fahrenheit we do a PATCH call like this.
::

	curl localhost:9090/ngsi-ld/v1/entities/house2%3Asmartrooms%3Aroom1/attrs/temperature -s -S -H 'Content-Type: application/json' -H 'Link: https://pastebin.com/raw/Mgxv2ykn' -d @- <<EOF
	{
		"value": 78,8,
		"unitCode": "FAH",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor0815"
		}
		"datasetId": "urn:fahrenheitentry:0815"
	}
	EOF

*************
Subscriptions
*************

NGSI-LD defines a subscription interface which allows you to get notifications on Entities. Subscriptions are on change subscriptions. This means you will not get a notification on an initial state of an entity as the result of a subscription. Subscriptions at the moment issue a notification when a matching Entity is created, updated or appended to. You will not get a notification when an Entity is deleted.

Subscribing to entities
#######################

In order to get the temperature of our rooms we will formulate a basic subscription which we can POST to the /ngsi-ld/v1/subscriptions endpoint.
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:1",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
	  }],
	  "notification": {
		"endpoint": {
			"uri": "http://ptsv2.com/t/30xad-1596541146/post",
			"accept": "application/json"
		}
	  },
	  "@context": ["https://pastebin.com/raw/Mgxv2ykn"]
	}
	EOF

As you can see entities is an array, which allows you to define multiple matching criteria for a subscription. You can subscribe by id or idPattern (regex) if you want. However a type is always mandatory in an entities entry.

Notification Endpoint
#####################

NGSI-LD currently supports two types of endpoints for subscriptions. HTTP(S) and MQTT(S). In the notification entry of a subscription you can define your endpoint with a uri and an accept MIME type. As you can see we are using an HTTP endpoint. 

Testing notification endpoint
#############################

For this example we are using Post Test Server V2 (http://ptsv2.com/). This is a public service without auth on our example. So be careful with your data. Also this service is meant for testing and debugging and NOT more. So be nice! They are giving us a good tool for development.
Normally you can use the example just as is. However if for some reason our endpoint is deleted please just go to ptsv2.com and click on "New Random Toilet" and replace the endpoint with the POST URL provided there.

Notifications
#############

Assuming that there is a temperature change in all of our rooms we will get 3 independent notifications, one for each change.
::

	{
		"id": "ngsildbroker:notification:-5983263741316604694",
		"type": "Notification",
		"data": [
			{
				"id": "house2:smartrooms:room1",
				"type": "urn:mytypes:room",
				"createdAt": "2020-08-04T12:55:05.276000Z",
				"modifiedAt": "2020-08-07T13:53:56.781000Z",
				"myuniqueuri:isPartOf": {
					"type": "Relationship",
					"createdAt": "2020-08-04T12:55:05.276000Z",
					"object": "smartcity:houses:house2",
					"modifiedAt": "2020-08-04T12:55:05.276000Z"
				},
				"myuniqueuri:temperature": {
					"type": "Property",
					"createdAt": "2020-08-04T12:55:05.276000Z",
					"providedBy": {
						"type": "Relationship",
						"createdAt": "2020-08-04T12:55:05.276000Z",
						"object": "smartbuilding:house2:sensor0815",
						"modifiedAt": "2020-08-04T12:55:05.276000Z"
					},
					"value": 22.0,
					"modifiedAt": "2020-08-04T12:55:05.276000Z"
				}
			}
		],
		"notifiedAt": "2020-08-07T13:53:57.640000Z",
		"subscriptionId": "urn:subscription:1"
	}

::

	{
		"id": "ngsildbroker:notification:-6853258236957905295",
		"type": "Notification",
		"data": [
			{
				"id": "house2:smartrooms:room2",
				"type": "urn:mytypes:room",
				"createdAt": "2020-08-04T11:17:28.641000Z",
				"modifiedAt": "2020-08-07T14:00:11.681000Z",
				"myuniqueuri:isPartOf": {
					"type": "Relationship",
					"createdAt": "2020-08-04T11:17:28.641000Z",
					"object": "smartcity:houses:house2",
					"modifiedAt": "2020-08-04T11:17:28.641000Z"
				},
				"myuniqueuri:temperature": {
					"type": "Property",
					"createdAt": "2020-08-04T11:17:28.641000Z",
					"providedBy": {
						"type": "Relationship",
						"createdAt": "2020-08-04T11:17:28.641000Z",
						"object": "smartbuilding:house2:sensor4711",
						"modifiedAt": "2020-08-04T11:17:28.641000Z"
					},
					"value": 23.0,
					"modifiedAt": "2020-08-04T11:17:28.641000Z"
				}
			}
		],
		"notifiedAt": "2020-08-07T14:00:12.475000Z",
		"subscriptionId": "urn:subscription:1"
	}
	
::
	{
		"id": "ngsildbroker:notification:-7761059438747425848",
		"type": "Notification",
		"data": [{
				"id": "house99:smartrooms:room42",
				"type": "urn:mytypes:room",
				"createdAt": "2020-08-04T13:19:17.512000Z",
				"modifiedAt": "2020-08-07T14:00:19.100000Z",
				"myuniqueuri:isPartOf": {
					"type": "Relationship",
					"createdAt": "2020-08-04T13:19:17.512000Z",
					"object": "smartcity:houses:house99",
					"modifiedAt": "2020-08-04T13:19:17.512000Z"
				},
				"myuniqueuri:temperature": {
					"type": "Property",
					"createdAt": "2020-08-04T13:19:17.512000Z",
					"providedBy": {
						"type": "Relationship",
						"createdAt": "2020-08-04T13:19:17.512000Z",
						"object": "smartbuilding:house99:sensor36",
						"modifiedAt": "2020-08-04T13:19:17.512000Z"
					},
					"value": 24.0,
					"modifiedAt": "2020-08-04T13:19:17.512000Z"
				}
			}
		],
		"notifiedAt": "2020-08-07T14:00:19.897000Z",
		"subscriptionId": "urn:subscription:1"
	}

As you can see we are getting now always the full Entity matching the type we defined in the subscription.

Subscribing to attributes
#########################

An alternative to get the same result in our setup is using the watchedAttributes parameter in a subscription. 
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:2",
	  "type": "Subscription",
	  "watchedAttributes": ["temperature"],
		"notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json"
			}
		},
	  "@context": "https://pastebin.com/raw/Mgxv2ykn"
	}
	EOF


This works in our example but you will get notifications everytime a temperature attribute changes. So in a real life scenario probably much more than we wanted.
You need to have at least the entities parameter (with a valid entry in the array) or the watchedAttributes parameter for a valid subscription. But you can also combine both. So if we want to be notified on every change of "temperature" in a "Room" we subscribe like this.
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:3",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
	  }],
	  "watchedAttributes": ["temperature"],
		"notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json"
			}
		},
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

We can now limit further down what we exactly we want to get in the notification very similar to the query.

IdPattern
#########

As we get now also the "Room" from smartcity:houses:house99 but we are only in interested smartcity:houses:house2 we will use the idPattern parameter to limit the results. This is possible in our case because of our namestructure. 
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:4",
	  "type": "Subscription",
	  "entities": [{
			"idPattern" : "house2:smartrooms:room.*",
			"type": "Room"
		}],
	  "watchedAttributes": ["temperature"],
	  "notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json"
			}
	  },
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF
 

Q Filter
########

Similar to our query we can also use the q filter to achieve this via the isPartOf relationship. Mind here in the body there is no URL encoding.
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:5",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
		}],
	  "q": "isPartOf==smartcity.houses.house2",
	  "watchedAttributes": ["temperature"],
		"notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json"
			}
		},
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

Reduce attributes
#################

Now since we still get the full Entity in our notifications we want to reduce the number of attributes. This is done by the attributes parameter in the notification entry.
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:6",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
		}],
	  "q": "isPartOf==smartcity.houses.house2",
	  "watchedAttributes": ["temperature"],
	  "notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json"
			},
			"attributes": ["temperature"]
	  },
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

As you can see, we now only get the temperature when the temperature changes.
::

	{
		"id": "ngsildbroker:notification:-7761059438747425848",
		"type": "Notification",
		"data": [
			{
				"id": "house2:smartrooms:room1",
				"type": "urn:mytypes:room",
				"createdAt": "2020-08-04T13:19:17.512000Z",
				"modifiedAt": "2020-08-07T14:30:12.100000Z",
				"myuniqueuri:temperature": {
					"type": "Property",
					"createdAt": "2020-08-04T13:19:17.512000Z",
					"providedBy": {
						"type": "Relationship",
						"createdAt": "2020-08-04T13:19:17.512000Z",
						"object": "smartbuilding:house99:sensor36",
						"modifiedAt": "2020-08-04T13:19:17.512000Z"
					},
					"value": 24.0,
					"modifiedAt": "2020-08-04T13:19:17.512000Z"
				}
			}
		],
		"notifiedAt": "2020-08-07T14:00:19.897000Z",
		"subscriptionId": "urn:subscription:6"
	}
	
The attributes and the watchedAttributes parameter can very well be different. If you want to know in which house a temperature changes you would subscribe like this
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:7",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
		}],
	  "watchedAttributes": ["temperature"],
		"notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json"
			},
			"attributes": ["isPartOf"]
		},
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

GeoQ filter
###########

An additional filter is the geoQ parameter allowing you to define a geo query. If, for instance, we want to be informend about all Houses near to a point we would subscribe like this.
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:8",
	  "type": "Subscription",
	  "entities": [{
			"type": "House"
		}],
	  "geoQ": {
	  "georel": {
		"near;maxDistance==2000",
		"geometry": "Point",
		"coordinates": [-8.50000005, 41.20000005]
	  },
		"notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json"
			},
			"attributes": ["isPartOf"]
		},
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

Additional endpoint parameters
##############################

The notification entry has two additional optional entries. receiverInfo and notifierInfo. They are both an array of a simple key value set.
Practically they represent settings for Scorpios notifier (notifierInfo) and additional headers you want to be sent with every notification (receiverInfo).
notifierInfo is currently only used for MQTT. 
If you want to, for instance, pass on an oauth token you would do a subscription like this 
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:9",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
		}],
	  "notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json",
				"receiverInfo": [{"Authorization": "Bearer sdckqk3123ykasd723knsws"}]
			}		
		},
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

MQTT endpoint
#############

If you have a running MQTT bus available, you can also get notifications to a topic on MQTT. However the setup of the MQTT bus and the creation of the topic is totaly outside of the responsibilities of an NGSI-LD broker.
An MQTT bus address must be provided via the URI notation of MQTT. mqtt[s]://[<username>:<password>@]<mqtt_host_name>:[<mqtt_port>]/<topicname>[[/<subtopic>]...]
So a subscription would generally look like this.
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:10",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
		}],
		"notification": {
			"endpoint": {
				"uri": "mqtt://localhost:1883/notifytopic",
				"accept": "application/json"
			}
		},
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

MQTT parameters
###############

MQTT has a few client settings which have to be configured. We do have some reasonable defaults here, if you don't provide it, but to be sure you better configure the client completly. These params are provided via the notifierInfo entry in endpoint.
Currently supported is 
"MQTT-Version" with possible values "mqtt3.1.1" or "mqtt5.0", default "mqtt5.0"
"MQTT-QoS" with possible values 0, 1, 2. Default 1.
Changing this to version 3.1.1 and QoS to 2 you would subscribe like this 
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:11",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
		}],
		"notification": {
			"endpoint": {
				"uri": "mqtt://localhost:1883/notifytopic",
				"accept": "application/json",
				"notifierInfo": [{"MQTT-Version": "mqtt3.1.1"},{"MQTT-QoS": 2}]
			}
		},
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

MQTT notifications
##################

Since MQTT is missing the header that HTTP callbacks have the format of a notification is slightly changed. Consisting of a metadata and a body entry. 
The metadata holds what is normally delivered via HTTP headers and the body contains the normal notification payload.
::

	{
		"metadata": {
			"Content-Type": "application/json"
			"somekey": "somevalue"
		},
		"body":
				{
					"id": "ngsildbroker:notification:-5983263741316604694",
					"type": "Notification",
					"data": [
						{
							"id": "house2:smartrooms:room1",
							"type": "urn:mytypes:room",
							"createdAt": "2020-08-04T12:55:05.276000Z",
							"modifiedAt": "2020-08-07T13:53:56.781000Z",
							"myuniqueuri:isPartOf": {
								"type": "Relationship",
								"createdAt": "2020-08-04T12:55:05.276000Z",
								"object": "smartcity:houses:house2",
								"modifiedAt": "2020-08-04T12:55:05.276000Z"
							},
							"myuniqueuri:temperature": {
								"type": "Property",
								"createdAt": "2020-08-04T12:55:05.276000Z",
								"providedBy": {
									"type": "Relationship",
									"createdAt": "2020-08-04T12:55:05.276000Z",
									"object": "smartbuilding:house2:sensor0815",
									"modifiedAt": "2020-08-04T12:55:05.276000Z"
								},
								"value": 22.0,
								"modifiedAt": "2020-08-04T12:55:05.276000Z"
							}
						}
					],
					"notifiedAt": "2020-08-07T13:53:57.640000Z",
					"subscriptionId": "urn:subscription:1"
				}
	}
	
****************
Batch operations
****************

NGSI-LD defines 4 endpoints for 4  batch operations. You can create a batch of Entity creations, updates, upserts or deletes.
Create, update and upsert are basically an array of the corresponding single Entity operations.
Assuming we want to create a few rooms for house 99 we would create the entities like this
::

	curl localhost:9090/ngsi-ld/v1/entityOperations/create -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	[{
			"id": "house99:smartrooms:room1",
			"type": "Room",
			
			"isPartOf": {
				"type": "Relationship",
				"object": "smartcity:houses:house99"
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		},
		{
			"id": "house99:smartrooms:room2",
			"type": "Room",
			"isPartOf": {
				"type": "Relationship",
				"object": "smartcity:houses:house99"
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		},
		{
			"id": "house99:smartrooms:room3",
			"type": "Room",
			"isPartOf": {
				"type": "Relationship",
				"object": "smartcity:houses:house99"
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		},
		{
			"id": "house99:smartrooms:room4",
			"type": "Room",
			"temperature": {
				"value": 21,
				"unitCode": "CEL",
				"type": "Property",
				"providedBy": {
					"type": "Relationship",
					"object": "smartbuilding:house99:sensor20041113"
				}
			},
			"isPartOf": {
				"type": "Relationship",
				"object": "smartcity:houses:house99"
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		}
	]
	EOF

Now as we did only add one temperature entry we are going to upsert the temperature for all the rooms like this.
::

	curl localhost:9090/ngsi-ld/v1/entityOperations/upsert -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	[{
			"id": "house99:smartrooms:room1",
			"type": "Room",
			"temperature": {
				"value": 22,
				"unitCode": "CEL",
				"type": "Property",
				"providedBy": {
					"type": "Relationship",
					"object": "smartbuilding:house99:sensor19970309"
				}
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		},
		{
			"id": "house99:smartrooms:room2",
			"type": "Room",
			"temperature": {
				"value": 23,
				"unitCode": "CEL",
				"type": "Property",
				"providedBy": {
					"type": "Relationship",
					"object": "smartbuilding:house99:sensor19960913"
				}
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		},
		{
			"id": "house99:smartrooms:room3",
			"type": "Room",
			"temperature": {
				"value": 21,
				"unitCode": "CEL",
				"type": "Property",
				"providedBy": {
					"type": "Relationship",
					"object": "smartbuilding:house99:sensor19931109"
				}
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		},
		{
			"id": "house99:smartrooms:room4",
			"type": "Room",
			"temperature": {
				"value": 22,
				"unitCode": "CEL",
				"type": "Property",
				"providedBy": {
					"type": "Relationship",
					"object": "smartbuilding:house99:sensor20041113"
				}
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		}
	]
	EOF

Now as we are at the end let's clean up with a batch delete. A batch delete is an array of Entity IDs you want to delete.
::

	curl localhost:9090/ngsi-ld/v1/entityOperations/delete -s -S -H 'Content-Type: application/json' -d @- <<EOF
	[
		"house99:smartrooms:room1",
		"house99:smartrooms:room2",
		"house99:smartrooms:room3",
		"house99:smartrooms:room4"
	]
	EOF

****************
Context Registry
****************

Next to the create, append, update interfaces which are used by Context Producers there is another concept in NGSI-LD which is the Context Source.
A Context Source is a source that provides the query and the subscription interface of NGSI-LD. 
For all intents and purposes an NGSI-LD Broker is by itself an NGSI-LD Context Source. This allows you a lot of flexibility when you want to have distributed setup.
Now in order to discover these Context Sources, the Context Registry is used, where Context Sources are registered in Scorpio.
Assuming we have an external Context Source which provides information about another house, we register it in the system like this:
::

	{
	  "id": "urn:ngsi-ld:ContextSourceRegistration:csr1a3458",
	  "type": "ContextSourceRegistration",
	  "information": [
		{
		  "entities": [
			{
			  "type": "Room"
			}
		  ]
		}
	  ],
	  "endpoint": "http://my.csource.org:1234",
	  "location": { "type": "Polygon", "coordinates": [[[8.686752319335938,49.359122687528746],[8.742027282714844,49.3642654834877],[8.767433166503904,49.398462568451485],[8.768119812011719,49.42750021620163],[8.74305725097656,49.44781634951542],[8.669242858886719,49.43754770762113],[8.63525390625,49.41968407776289],[8.637657165527344,49.3995797187007],[8.663749694824219,49.36851347448498],[8.686752319335938,49.359122687528746]]] },
	  "@context": "https://pastebin.com/raw/Mgxv2ykn"
	}

Now Scorpio will take the registered Context Sources which are have a matching registration into account on its queries and subscriptions.
You can also independently query or subscribe to the context registry entries, quite similar to the normal query or subscription, and interact with the Context Sources independently.
Now if we query for all registrations which provide anything of type Room like this 
::

	curl localhost:9090/ngsi-ld/v1/csourceRegistrations/?type=Room -s -S -H 'Accept: application/json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"' 

we will get back our original registration and everything that has been registered with the type Room.

Context Registry usage for normal queries & subscriptions
#########################################################

A context registry entry can have multiple entries which are taken into consideration when normal queries or subscriptions arrive in Scorpio.
As you can see there is an entities entry similar to the one in the subscriptions. This is the first thing to be taken into consideration.
If you register a type, Scorpio will only forward a request which is matching that type. Similarly the location is used to decide if a query with geo query part should be forwarded. While you shouldn't overdo it, the more details you provide in a registration the more efficiently your system will be able to determine to which context source a request should be forwarded to.
Below you see an example with more properties set.
::

	{
	  "id": "urn:ngsi-ld:ContextSourceRegistration:csr1a3459",
	  "type": "ContextSourceRegistration",
	  "name": "NameExample",
	  "description": "DescriptionExample",
	  "information": [
		{
		  "entities": [
			{
			  "type": "Vehicle"
			}
		  ],
		  "properties": [
			"brandName",
			"speed"
		  ],
		  "relationships": [
			"isParked"
		  ]
		},
		{
		  "entities": [
			{
			  "idPattern": ".*downtown$",
			  "type": "OffStreetParking"
			}
		  ]
		}
	  ],
	  "endpoint": "http://my.csource.org:1026",
	  "location": "{ \"type\": \"Polygon\", \"coordinates\": [[[8.686752319335938,49.359122687528746],[8.742027282714844,49.3642654834877],[8.767433166503904,49.398462568451485],[8.768119812011719,49.42750021620163],[8.74305725097656,49.44781634951542],[8.669242858886719,49.43754770762113],[8.63525390625,49.41968407776289],[8.637657165527344,49.3995797187007],[8.663749694824219,49.36851347448498],[8.686752319335938,49.359122687528746]]] }"
	}

There are two entries in the information part. In the first you can see there are two additional entries describing the two properties and one relationship provided by that source. That means any query which asks for type Vehicle, without an attribute filter, will be forwarded to this source and if there is an attribute filter it will only be forwarded if the registered properties or relationships match. The second entry means that this source can provide Entities of type OffStreetParking, which have an Entity ID ending with "downtown". 
