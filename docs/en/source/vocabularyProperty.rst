*****************************
VocabularyProperty Support
*****************************

**NGSI-LD Vocabulary:** string representation of a main characteristic which is explicitly defined to undergo JSON-LD type coercion to a URI.

The VocabularyProperty support feature enables the uniqueness of the value and allows user to provide different short terms to the associated value, i.e., a different @context can map it to a different language.

The value of a Vocabulary Property is expanded to a URI if the term is defined in the JSON-LD @context.

To enable this behavior, the JSON-LD key has to be defined as @vocab in the JSON-LD context. Thus, a Vocabulary Property has a **“vocab”**, instead of the "value” of a regular Property.

For Example:
::

	"category": {
		"type": "VocabularyProperty",
		"vocab": "non-commercial"
	}
	

where the @context contains - "non-commercial": "http://example.org/vehicle/non-commercial"

We can map a different @context which can be used to map it to a different language, e.g., "nicht-kommerziell": "http://example.org/vehicle/non-commercial"


Example for VocabularyProperty support
-----------------------------------------

1. Hosting multiple @context
=============================

Here, we are hosting two different @context objects that can be used to enables the uniqueness of the value and allows user to provide different short terms to the associated value.

- Host First Context

.. code-block:: JSON

	curl --location --request POST 'http://localhost:9090/ngsi-ld/v1/jsonldContexts' \
	--header 'Content-Type: application/json' \
	--data-raw ‘{
	    "@context": {
	        "non-commercial": "https://uri.etsi.org/ngsi-ld/default-context/non-commercial"
	    }
	}
	’

- Host Second Context

.. code-block:: JSON

	curl --location --request POST 'http://localhost:9090/ngsi-ld/v1/jsonldContexts' \
	--header 'Content-Type: application/json' \
	--data-raw ‘{
	    "@context": {
	        "nicht-kommerziell": "https://uri.etsi.org/ngsi-ld/default-context/non-commercial"
	    }
	}
	’

2. Create Operation
=====================

In order to create the entity, we can hit the endpoint POST - **http://<IP Address>:<port>/ngsi-ld/v1/entities/**  with the given payload

.. code-block:: JSON

	{
		"id": "urn:ngsi-ld:Vehicle:A4567",
		"type": "Vehicle",
		"brandName": {
			"type": "Property",
			"value": "Mercedes"
		},
		"isParked": {
			"type": "Relationship",
			"object": "urn:ngsi-ld:OffStreetParking:Downtown1",
			"observedAt": "2017-07-29T12:00:04Z"
		},
		"category": {
			"type": "VocabularyProperty",
			"vocab": "non-commercial"
		}
	}

	
3. Query Operation
====================

- In order to retrieve the Entity details with First Hosted @context, make a GET request providing our @context via the 'Link' like this

curl --location 'http://localhost:9090/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A4567' \
--header 'Accept: application/json' \
--header 'Link: <http://localhost:9090/ngsi-ld/v1/jsonldContexts/urn:20906179-533a-4384-b8a3-ab9be6f9604a>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'

**Note:** We are hosting our own @context as given in Step 1, we can provide it via the 'Link' header.

Response:

.. code-block:: JSON

	{
		"id": "urn:ngsi-ld:Vehicle:A4567",
		"type": "Vehicle",
		"brandName": {
			"type": "Property",
			"value": "Mercedes"
		},
		"isParked": {
			"type": "Relationship",
			"object": "urn:ngsi-ld:OffStreetParking:Downtown1",
			"observedAt": "2017-07-29T12:00:04Z"
		},
		"category": {
			"type": "VocabularyProperty",
			"vocab": "non-commercial"
		}
	}

- In order to retrieve the Entity details with Second Hosted @context, make a GET request providing our @context via the 'Link' like this

curl --location 'http://localhost:9090/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A4567' \
--header 'Accept: application/json' \
--header 'Link: <http://localhost:9090/ngsi-ld/v1/jsonldContexts/urn:73f6f47b-7d0b-49dc-b9f6-697b7443226f>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'

Response:

.. code-block:: JSON

	{
		"id": "urn:ngsi-ld:Vehicle:A4567",
		"type": "Vehicle",
		"brandName": {
			"type": "Property",
			"value": "Mercedes"
		},
		"isParked": {
			"type": "Relationship",
			"object": "urn:ngsi-ld:OffStreetParking:Downtown1",
			"observedAt": "2017-07-29T12:00:04Z"
		},
		"category": {
			"type": "VocabularyProperty",
			"vocab": "nicht-kommerziell"
		}
	}

Here VocabularyProperty enables the uniqueness of the value and allows to use different short names for the associated value.