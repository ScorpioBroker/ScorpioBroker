---
type: NGSI-LD Resource
title: "Resource: temporal/entityMaps"
description: "6.35.1 Description This resource is used for creating entityMaps based on temporal queries in an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.35
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.35"
---

6.35.1 Description

This resource is used for creating entityMaps based on temporal queries in an NGSI-LD system.

6.35.2 Resource definition

Resource URI: - /temporal/entityMaps/

6.35.3 Resource methods

6.35.3.1 GET
This method is associated to the operation "Create EntityMap for Query Temporal Evolution of Entities" and shall
exhibit the behaviour defined by clause 5.14.5, an EntityMap as part of the HTTP response payload body. In addition to
this method, an alternative way to perform "Create EntityMap for Query Temporal Evolution of Entities" operations via
POST is defined in clause 6.35.3.2. Figure 6.35.3.1-1 shows this interaction.

Figure 6.35.3.1-1: Create EntityMap for Query Temporal Evolution of Entities interaction

The URL parameters that shall be supported by implementations are the same as those for Query Temporal Evolution of Entities and can be found in Table 6.18.3.2-1. Table 6.35.3.1-1 describes the request body and possible responses.


Table 6.35.3.1-1: Create EntityMap for Query Temporal Evolution of Entities request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Response Body

Data Type Cardinality Response Codes Remarks EntityMap 1 201 Created A response body containing the entityMap with the identifiers of the Entities matching the query. The HTTP response shall include an "NGSILD-EntityMap" HTTP header that contains the resource URI of the EntityMap resource created in the operation.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

6.35.3.2 POST
This method is associated to the operation "Create EntityMap for Query Temporal Evolution of Entities" and shall
exhibit the behaviour defined by clause 5.14.5. Figure 6.35.3.2-1 shows the operation interaction and Table 6.35.3.2-1
describes the request body and possible responses.

Figure 6.35.3.2-1: Create EntityMap for Query Temporal Evolution of Entities via POST interaction

Table 6.35.3.2-1: Create EntityMap for Query Temporal Evolution of Entities via POST request body and possible responses

Request Body

Data Type Cardinality Remarks Query 1 Payload body in the request contains a JSON-LD object which represents the query to be performed.

Response Body

Data Type Cardinality Response Codes Remarks EntityMap 1 201 Created A response body containing the entityMap with the identifiers of the Entities matching the query. The HTTP response shall include an "NGSILD-EntityMap" HTTP header that contains the resource URI of the EntityMap resource created in the operation.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

# Related

* [Operation: Create EntityMap for Query Temporal Evolution of Entities](/api-operations/entity-mapping/create-entitymap-for-query-temporal-evolution-of-entities.md)
* [Clause 5.14.5](/api-operations/entity-mapping/create-entitymap-for-query-temporal-evolution-of-entities.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.35](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: temporal/entityMaps
