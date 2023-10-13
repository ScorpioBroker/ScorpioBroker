*********************************************
Aggregated Temporal Representation of Entity
*********************************************

NGSI-LD specification defines an alternative temporal representation of Entities, called Aggregated Temporal Representation, which allows consuming temporal Entity data after applying an aggregation function on the values of the Attribute instances.

The aggregation function is applied according to the following principles:

 • An aggregation method specifies the function used to aggregate the values (e.g. sum, mean, max, etc.). A Context Consumer can ask for many aggregation methods in one request.
 • The duration of an aggregation period specifies the duration of each period to be used when applying the aggregation function on the values of a Temporal Entity.

In order to support such aggregation functions, two parameters are defined:

1. **aggrMethods**, to express the aggregation methods to apply.
 The values supported by the aggrMethods parameter are the following:
  • aggrMethods = *"totalCount" / "distinctCount" / "sum" / "avg" / "min" / "max" / "stddev" / "sumsq"*

2. **aggrPeriodDuration**, to express the duration of the period to consider in each step of the aggregation.
 The duration is expressed using the following format and conventions:
  • The duration shall be a string in the format P[n]Y[n]M[n]DT[n]H[n]M[n]S or P[n]W, where [n] is replaced by the value for each of the date and time elements that follow the [n], P is the duration designator and T is the time designator. For example, "P3Y6M4DT12H30M5S" represents a duration of "three years, six months, four days, twelve hours, thirty minutes, and five seconds".
  • A duration of 0 second (e.g. expressed as "PT0S" or "P0D") is valid and is interpreted as a duration spanning the whole-time range specified by the temporal query.
  • Alternative representations based on combined date and time representations are not allowed.


Example for Aggregation Support
---------------------------------

1. Create Operation
====================

In order to create an entity, we can hit the endpoint POST **http://<IP Address>:<port>/ngsi-ld/v1/entities** with the given payload.

Payload:

.. code-block:: JSON

 {
     "id": "urn:ngsi-ld:Vehicle:B9211",
     "type": "Vehicle",
     "brandName": [
         {
             "type": "Property",
             "value": "Mercedes"
         }
     ],
     "speed": [
         {
             "type": "Property",
             "value": 120,
             "observedAt": "2020-08-01T12:00:00Z"
         },
         {
             "type": "Property",
             "value": 100,
             "observedAt": "2020-08-01T12:01:00Z"
         },
         {
             "type": "Property",
             "value": 95,
             "observedAt": "2020-08-01T12:02:00Z"
         },
         {
             "type": "Property",
             "value": 85,
             "observedAt": "2020-08-01T12:03:00Z"
         },
         {
             "type": "Property",
             "value": 100,
             "observedAt": "2020-08-01T12:04:00Z"
         },
         {
             "type": "Property",
             "value": 115,
             "observedAt": "2020-08-01T12:05:00Z"
         },
         {
             "type": "Property",
             "value": 100,
             "observedAt": "2020-08-01T12:06:00Z"
         },
         {
             "type": "Property",
             "value": 80,
             "observedAt": "2020-08-01T12:07:00Z"
         },
         {
             "type": "Property",
             "value": 55,
             "observedAt": "2020-08-01T12:08:00Z"
         }
     ],
     "@context": [
         "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.6.jsonld"
     ]
 }


2. Query Operation
===================

To retrieve entity with aggregation support, you can send an HTTP GET to - **http://<IP Address>:<port>/ngsi-ld/v1/entities?aggrMethods={aggrMethods}** and will return back the Entities with given aggregated methods.

**EXAMPLE:** Give back all aggr methods (max, min, avg, sum, sumsq, stddev, totalCount, distinctCount) for Entities of type Vehicle

	GET - **http://localhost:9090/ngsi-ld/v1/temporal/entities?type=Vehicle&aggrMethods=max%2Cmin%2Cavg%2Csum%2Csumsq%2Cstddev%2CtotalCount%2CdistinctCount&timeproperty=observedAt'**

Response:

.. code-block:: JSON

 [
     {
         "id": "urn:ngsi-ld:Vehicle:B9211",
         "type": "Vehicle",
         "brandName": {
             "type": "Property",
             "distinctCount": [
                 [
                     1,
                     "2020-08-01T11:00:00.000000Z",
                     "2020-08-01T11:00:00.000000Z"
                 ]
             ],
             "max": [
                 [
                     "Mercedes",
                     "2020-08-01T11:00:00.000000Z",
                     "2020-08-01T11:00:00.000000Z"
                 ]
             ],
             "min": [
                 [
                     "Mercedes",
                     "2020-08-01T11:00:00.000000Z",
                     "2020-08-01T11:00:00.000000Z"
                 ]
             ],
             "totalCount": [
                 [
                     1,
                     "2020-08-01T11:00:00.000000Z",
                     "2020-08-01T11:00:00.000000Z"
                 ]
             ]
         },
         "speed": {
             "type": "Property",
             "avg": [
                 [
                     94.44444444444444,
                     "2020-08-01T12:00:00.000000Z",
                     "2020-08-01T12:08:00.000000Z"
                 ]
             ],
             "distinctCount": [
                 [
                     7,
                     "2020-08-01T12:00:00.000000Z",
                     "2020-08-01T12:08:00.000000Z"
                 ]
             ],
             "max": [
                 [
                     95,
                     "2020-08-01T12:00:00.000000Z",
                     "2020-08-01T12:08:00.000000Z"
                 ]
             ],
             "min": [
                 [
                     100,
                     "2020-08-01T12:00:00.000000Z",
                     "2020-08-01T12:08:00.000000Z"
                 ]
             ],
             "stddev": [
                 [
                     19.436506316151,
                     "2020-08-01T12:00:00.000000Z",
                     "2020-08-01T12:08:00.000000Z"
                 ]
             ],
             "sum": [
                 [
                     850,
                     "2020-08-01T12:00:00.000000Z",
                     "2020-08-01T12:08:00.000000Z"
                 ]
             ],
             "sumsq": [
                 [
                     83300.0,
                     "2020-08-01T12:00:00.000000Z",
                     "2020-08-01T12:08:00.000000Z"
                 ]
             ],
             "totalCount": [
                 [
                     9,
                     "2020-08-01T12:00:00.000000Z",
                     "2020-08-01T12:08:00.000000Z"
                 ]
             ]
         },
         "@context": [
             "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.6.jsonld"
         ]
     }
 ]