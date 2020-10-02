*******************
Hello World example
*******************

Generally speaking you can Create entities which is like the hello world program for Scorpio Broker by sending an HTTP POST request to *http://localhost:9090/ngsi-ld/v1/entities/* with a payload like this

.. code-block:: JSON

 curl localhost:9090/ngsi-ld/v1/entities -s -S -H 'Content-Type: application/json' -d @-
 {
    "id": "urn:ngsi-ld:testunit:123",
    "type": "AirQualityObserved",
    "dateObserved": {
        "type": "Property",
        "value": {
            "@type": "DateTime",
            "@value": "2018-08-07T12:00:00Z"
        }
    },
    "NO2": {
        "type": "Property",
        "value": 22,
        "unitCode": "GP",
        "accuracy": {
            "type": "Property",
            "value": 0.95
        }
    },
    "refPointOfInterest": {
        "type": "Relationship",
        "object": "urn:ngsi-ld:PointOfInterest:RZ:MainSquare"
    },
    "@context": [
        "https://schema.lab.fiware.org/ld/context",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    ]
 }

In the given example the @context is in the payload therefore you have to set the ContentType header to application/ld+json

To receive entities you can send an HTTP GET to

 **http://localhost:9090/ngsi-ld/v1/entities/<entityId>**