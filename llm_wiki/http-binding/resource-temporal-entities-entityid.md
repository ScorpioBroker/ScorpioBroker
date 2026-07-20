---
type: NGSI-LD Resource
title: "Resource: temporal/entities/{entityId}"
description: "6.19.1 Description This resource is associated to the Temporal Evolution of an Entity known to an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.19
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.19"
---

6.19.1 Description

This resource is associated to the Temporal Evolution of an Entity known to an NGSI-LD system.

6.19.2 Resource definition

Resource URI:

- /temporal/entities/{entityId} Resource URI variables for this resource are defined in Table 6.19.2-1.

Table 6.19.2-1: URI variables Name Definition entityId Id (URI) of the entity to be retrieved

6.19.3 Resource methods

6.19.3.1 GET
This method is associated to the operation "Retrieve Temporal Evolution of an Entity" and shall exhibit the behaviour
defined by clause 5.7.3. The Entity identifier is the value of the resource URI variable entityId. Figure 6.19.3.1-1 shows
the retrieve temporal representation of an entity interaction.


Figure 6.19.3.1-1: Retrieve Temporal Evolution of an Entity interaction

The URL parameters that shall be supported are those defined in Table 6.19.3.1-1 and Table 6.19.3.1-2 describes the request body and possible responses.

Table 6.19.3.1-1: Retrieve Temporal Evolution of an Entity URL parameters Name Data Type Cardinality Remarks aggrMethods Comma separated list of strings

0..1 It shall be 1 if aggregatedValues is present in the options parameter

Each String represents the aggregation methods as defined by clause 4.5.19. Only applicable if "aggregatedValues" is present in the format or options parameter. aggrPeriodDuration String 0..1 It represents the duration of each period used for the aggregation as defined by clause 4.5.19. If not specified, it defaults to a duration of 0 seconds and is interpreted as a duration spanning the whole time-range specified by the temporal query. Only applicable if "aggregatedValues" is present in the format or options parameter.

attrs Comma separated list of strings

0..1 A synonym for the pick parameter, except that id, type, scope are not allowed. Deprecated

Each String is an Attribute (Property or Relationship) name. List of Attributes to be retrieved. If not specified, all Attributes related to the temporal representation of an Entity shall be retrieved.

datasetId Comma separated list of strings

0..1 Shall be valid URIs, "@none" for including the default Attribute instances. Specifies the datasetIds of the Attribute instances to be selected for each matched Attribute as per clause 4.5.5.

endTimeAt String 0..1 It shall be 1 if timerel is equal to "between"

It represents the endTimeAt parameter as
defined by clause 4.11.
It shall be a DateTime.
lang String 0..1 It represents the preferred natural language of
the response.
It is used to reduce languageMaps to a string or
string array property in a single preferred
language.
lastN Positive integer 0..1 Only the last n Attribute instances (under the
concerned time interval) shall be retrieved.

omit Comma separated list of strings

0..1 Each String is an Entity member ("id", "type", "scope" or an Attribute name). When defined, the listed Entity members are removed from each Entity within the payload.

Name Data Type Cardinality Remarks pick Comma separated list of strings

0..1 Each String is an Entity member ("id", "type", "scope" or an Attribute name). When defined, every Entity within the payload body is reduced down to only contain the listed Entity members.

timeAt String 0..1 It shall be 1 if timerel is present

It represents the timeAt parameter as defined by clause 4.11. It shall be a DateTime. timeproperty String 0..1 It represents a Temporal Property name. Allowed values: "observedAt", "createdAt", "modifiedAt" and "deletedAt". If not specified, the default is "observedAt". (See clause 4.8).

| timerel | | String | 0..1 | | It represents the Temporal Relationship as | | |
| --- | --- | --- | --- | --- | --- | --- | --- |
| | | | It shall be 1 if timeAt | | defined by clause 4.11. | | |
| | | | is present | | Allowed values: "before", "after", | | |

"between".

Table 6.19.3.1-2: Get Temporal Evolution of an Entity request body and possible responses Request Body

Data Type Cardinality Remarks N/A N/A

Data Type Cardinality Response Codes Remarks EntityTemporal 1 200 OK A response body containing the JSON-LD temporal representation of the target Entity containing the selected Attributes.

EntityTemporal 1 203 Non Authoritative Information

As above, but returning an altered response body, amended to conform to a specific version of the NGSI-LD specification as mandated in clause 4.3.6.8. The response shall also include a "Preference-Applied" HTTP header set to "ngsi-ld= ".

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an Entity identifier (URI) not known to the system, see clause 6.3.2.

6.19.3.2 DELETE

This method is associated to the operation "Delete Temporal Evolution of an Entity" and shall exhibit the behaviour defined by clause 5.6.16. The Entity identifier is the value of the resource URI variable entityId. Figure 6.19.3.2-1 shows the delete entity interaction and Table 6.19.3.2-1 describes the request body and possible responses.


Figure 6.19.3.2-1: Delete Temporal Evolution of an Entity interaction

Table 6.19.3.2-1: Delete Temporal Evolution of an Entity request body and possible responses Request Body

Data Type Cardinality Remarks N/A N/A

Response Body

Data Type Cardinality Response Codes Remarks N/A N/A 204 No Content ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an Entity identifier (URI) not known to the system, see clause 6.3.2.

# Related

* [Operation: Delete Temporal Evolution of an Entity](/api-operations/provision/delete-temporal-evolution-of-an-entity.md)
* [Operation: Retrieve Temporal Evolution of an Entity](/api-operations/consumption/retrieve-temporal-evolution-of-an-entity.md)
* [Clause 4.11](/framework/languages/ngsi-ld-temporal-query-language.md)
* [Clause 4.3.6.8](/framework/architecture/backwards-compatibility-of-context-source-payloads.md)
* [Clause 4.5.19](/framework/data-representation/aggregated-temporal-representation-of-an-entity.md)
* [Clause 4.5.5](/framework/data-representation/multi-attribute-support.md)
* [Clause 4.8](/framework/geo-temporal/temporal-properties.md)
* [Clause 5.6.16](/api-operations/provision/delete-temporal-evolution-of-an-entity.md)
* [Clause 5.7.3](/api-operations/consumption/retrieve-temporal-evolution-of-an-entity.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.19](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: temporal/entities/{entityId}
