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
  

For example, **GET API:** *“/ngsi-ld/v1/temporal/entities?aggrMethods=max,avg&aggrPeriodDuration=PT4M&options=aggregatedValues”* 
 - This API will return back the maximum and average speed of Entities and aggregated by periods of 4 minutes.

**Temporal Query (Aggregated Representation)**
################################################

**EXAMPLE:** Give back the maximum and average speed of Entities of type Vehicle whose "brandName" attribute is "Mercedes" between the 1st of August at noon and the 1st of August at 01 PM, aggregated by periods of 4 minutes

**GET API**
------------
*http://localhost:9090/ngsi-ld/v1/temporal/entities/?type=Vehicle&q=brandName=Mercedes&attrs=speed&timerel=between&timeAt=2020-08-01T12:00:00Z&endTimeAt=2020-08-01T13:00:00Z&aggrMethods=max,avg&aggrPeriodDuration=PT4M&options=aggregatedValues*

Response:
::
	[
		{
			"id": "urn:ngsi-ld:Vehicle:B9211",
			"type": "Vehicle",
			"brandName": [
				{
					"type": "Property",
					"value": "Mercedes"
				}
			],
			"speed": {
				"type": "Property",
				"max": [
					[
						120,
						"2020-08-01T12:00:00Z",
						"2020-08-01T12:04:00Z"
					],
					[
						100,
						"2020-08-01T12:04:00Z",
						"2020-08-01T12:08:00Z"
					]
				],
				"avg": [
					[
						120,
						"2020-08-01T12:00:00Z",
						"2020-08-01T12:04:00Z"
					],
					[
						90,
						"2020-08-01T12:04:00Z",
						"2020-08-01T12:08:00Z"
					]
				]
			},
			"@context": [
				"http://example.org/ngsi-ld/latest/vehicle.jsonld",
				"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.6.jsonld"
			]
		}
	]