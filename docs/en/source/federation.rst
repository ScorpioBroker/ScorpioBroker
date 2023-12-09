***************************
Context Broker Federation
***************************

A "Federation Broker" is a Broker in a distributed setup with an associated Context Registry (e.g. - entity is created in one Scorpio instance and processed by other Scorpio instance without storing the data locally). This Context Providers and request forwarding functionality is a kind of "pull" federation (in which one Scorpio instance forwards a query/update to another Scorpio instance). 

**Let's illustrate with an example**

.. figure:: figures/federation.png

Consider the following setup: two context broker instances running in the same machine (of course, this is not a requirement but makes things simpler to test this feature), on ports 9090 and 9091 respectively and using different databases (named A and B to be brief).

Let's start each instance and consider current setup is as follows:

Federator CB is deployed on http://federator_ip:9090
Instance1 CB is deployed on http://instance1_ip:9091

**Step 1.** Create a simple entity in instance1

.. code-block:: JSON

 POST http://instance1_ip:9091/ngsi-ld/v1/entities
 Content-Type: application/ld+json
 Body:
 {
     "id": "urn:ngsi-ld:AirQuality:test1",
     "type": "AirQuality",
     "co": {
         "type": "Property",
         "value": 0.2,
         "observedAt": "2023-10-17T04:25:00Z",
         "unitCode": "GP"
     },
     "@context": [
         "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
     ]
 }


**Step 2.** Register the instance1 into the federator broker.

.. code-block:: JSON

 POST http://federator_ip:9090/ngsi-ld/v1/csourceRegistrations
 Content-Type: application/ld+json
 Body:
 {
     "id": "urn:ngsi-ld:ContextSourceRegistration:test001",
     "type": "ContextSourceRegistration",
     "information": [
         {
             "entities": [
                 {
                     "type": "AirQuality"
                 }
             ]
         }
     ],
     "endpoint": "http://instance1_ip:9091",
     "@context": [
         "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
     ]
 }
 
**Step 3.** Query the AirQuality entities in both Context Brokers.

- Instance1 Request:

.. code-block:: JSON

 GET http://instance1_ip:9091/ngsi-ld/v1/entities/?type=AirQuality
 Accept: application/ld+json

Response:

.. code-block:: JSON

 [
     {
         "id": "urn:ngsi-ld:AirQuality:test1",
         "type": "AirQuality",
         "co": {
             "type": "Property",
             "value": 0.2,
             "observedAt": "2023-10-17T04:25:00Z",
             "unitCode": "GP"
         },
         "@context": [
             "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
         ]
     }
 ]
 
- Federator Request:

.. code-block:: JSON

 GET http://federator_ip:9090/ngsi-ld/v1/entities/?type=AirQuality
 Accept: application/ld+json

Response:

.. code-block:: JSON

 [
     {
         "id": "urn:ngsi-ld:AirQuality:test1",
         "type": "AirQuality",
         "co": {
             "type": "Property",
             "value": 0.2,
             "observedAt": "2023-10-17T04:25:00Z",
             "unitCode": "GP"
         },
         "@context": [
             "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
         ]
     }
 ]
 
Here, we can see a simple use case of federation setup in which entity is located in *Instance1* Scorpio broker and forwarded to the *Federator* Scorpio Broker.