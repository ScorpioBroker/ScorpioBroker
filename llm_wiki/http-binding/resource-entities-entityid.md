---
type: NGSI-LD Resource
title: "Resource: entities/{entityId}"
description: "6.5.1 Description This resource represents an entity known to an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.5
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.5"
---

6.5.1 Description

This resource represents an entity known to an NGSI-LD system.


6.5.2 Resource definition

Resource URI:

- /entities/{entityId} Resource URI variables for this resource are defined in Table 6.5.2-1.

Table 6.5.2-1: URI variables Name Definition entityId Id (URI) of the entity to be retrieved

6.5.3 Resource methods

6.5.3.1 GET
This method is associated to the operation "Retrieve Entity" and shall exhibit the behaviour defined by clause 5.7.1. The
Entity identifier is the value of the resource URI variable "entityId". Figure 6.5.3.1-1 shows the retrieve entity
interaction.

Figure 6.5.3.1-1: Retrieve Entity interaction

The URL parameters that shall be supported are those defined in Table 6.5.3.1-1, the request headers that shall be supported by implementations are those defined in Table 6.5.3.1-2 and Table 6.5.3.1-3 describes the request body and possible responses.


Table 6.5.3.1-1: Retrieve Entity URL parameters Name Data Type Cardinality Remarks attrs Comma separated list of strings

0..1 A synonym for the pick parameter, except that id, type, scope are not allowed. Deprecated

Each String is an Attribute (Property or Relationship) name. List of Attributes to be matched by the Entity and included in the response. If the Entity does not have any of the Attributes in attrs, then a 404 Not Found shall be retrieved. If attrs is not specified, no matching is performed and all Attributes related to the Entity shall be retrieved.

containedBy Comma separated list of URIs

0..1 List of entity ids which have previously been encountered whilst retrieving the Entity Graph. Only applicable if joinLevel is present.

datasetId Comma separated list of strings

0..1 Shall be valid URIs, "@none" for including the default
Attribute instances. Specifies the datasetIds of the
Attribute instances to be selected for each matched
Attribute as per clause 4.5.5.
entityMap Boolean 0..1 If true, the location of the EntityMap used in the
operation is returned in the response.
entityMapLifetime String 0..1 Suggested duration, represented in ISO 8601 [17]
format, for which the requester wants the EntityMap to
exist. The actual expiresAt time of the EntityMap shall be
set by the Context Broker or Context Source, possibly
overriding the requested duration. Only applicable if
entityMap is set to true.
geometryProperty String 0..1 It represents a GeoProperty name.
In the case of GeoJSON Entity representation, this
parameter indicates which GeoProperty to use for the
"geometry" element. By default, it shall be location.
join String 0..1 The type of Linked Entity retrieval to apply (see
clause 4.5.23). Allowed values: "flat", "inline",
"@none" .
joinLevel Positive Integer 0..1 Depth of Linked Entity retrieval to apply. Default is
1
Only applicable if join parameter is: "flat" or
"inline".
lang String 0..1 It represents the preferred natural language of the
response.
It is used to reduce languageMaps to a string or string
array property in a single preferred language.

omit Comma separated list of strings

0..1 Each String is an Entity member ("id", "type", "scope" or a projected Attribute name). When defined, the listed Entity members are removed from the Entity (applies to all Entities in the payload in the case of Linked Entity retrieval).

pick Comma separated list of strings

0..1 Each String is an Entity member ("id", "type", "scope" or a projected Attribute name). When defined, the Entity is reduced down to only contain the listed Entity members (applies to all Entities in the payload in the case of Linked Entity retrieval). type String 0..1 Selection of Entity Types as per clause 4.17.

Table 6.5.3.1-2: Retrieve Entity request Headers Name Data Type Cardinality Remarks NGSILD-EntityMap URI 0..1 If present, the EntityMap supplied is used for determining the extent of the Entity data requested during the retrieval operation. The location of the EntityMap used in the retrieval operation is returned in the response.

Table 6.5.3.1-3: Retrieve Entity request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Data Type Cardinality Response Codes Remarks Entity 1 200 OK A response body containing the JSON-LD representation of the target entity which consists only of the selected Attributes, unless the Accept Header indicates that the Entity is to be rendered as GeoJSON. If an EntityMap has been requested, the HTTP response shall include an "NGSILD-EntityMap" HTTP header that contains the resource URI of the EntityMap resource used in the operation.

Entity 1 203 Non Authoritative Information

As above, but returning an altered response body, amended to conform to a specific version of the NGSI-LD specification as mandated in clause 4.3.6.8. The response shall also include a "Preference-Applied" HTTP header set to "ngsi ld= ". GeoJSON Feature 1 200 OK If the Accept Header indicates that the Entity is to be rendered as GeoJSON, a GeoJSON Feature is returned. If an EntityMap has been requested, the HTTP response shall include an "NGSILD-EntityMap" HTTP header that contains the resource URI of the EntityMap resource used in the operation.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an Entity identifier (URI) not known to the system, see clause 6.3.2.

| | ProblemDetails (see | | 1 | | 501 Not | | | It is used by Registered Context | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | IETF RFC 7807 [10]) | | | | Implemented | | | Sources to indicate that the data format | | | |
| | | | | | | | | of the request is unsupported see clause | | | |

6.3.7.

6.5.3.2 DELETE

This method is associated to the operation "Delete Entity" and shall exhibit the behaviour defined by clause 5.6.6. The Entity identifier is the value of the resource URI variable "entityId". Figure 6.5.3.2-1 shows the delete entity interaction.


Figure 6.5.3.2-1: Delete Entity interaction

The URL parameters that shall be supported are those defined in Table 6.5.3.2-1 and Table 6.5.3.2-2 describes the request body and possible responses.

Table 6.5.3.2-1: Delete Entity URL parameters Name Data Type Cardinality Remarks type String 0..1 Selection of Entity Types as per clause 4.17.

Table 6.5.3.2-2: Delete Entity request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Remarks

Data Type Cardinality Response Codes

| | | N/A | | N/A | | 204 No | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | | | Content | | | |
| | | BatchOperation | | 1 | | 207 Multi | | If the entity input data matches to a | |
| | | Result | | | | Status | | registration, the relevant parts of the request | |

If the entity input data matches to a registration, the relevant parts of the request are forwarded as a distributed operation. In the case when an error response is received back from any distributed operation, a response body containing the result returned from each registration is returned in a BatchOperationResult structure. Errors can occur whenever a distributed operation is unsupported, fails or times out, see clause 6.3.17.

Response Body

1 400 Bad Request

ProblemDetails (see IETF RFC 7807 [10])

It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

1 404 Not Found

ProblemDetails (see IETF RFC 7807 [10])

It is used when a client provided an Entity identifier (URI) not known to the system, see clause 6.3.2.

6.5.3.3 PUT
This method is bound to the "Replace Entity" operation and shall exhibit the behaviour defined by clause 5.6.18. The
Entity identifier is the value of the resource URI variable "entityId". The data to be updated shall be contained in the
HTTP request payload body. Figure 6.5.3.3-1 shows the Replace Entity interaction.


Figure 6.5.3.3-1: Replace Entity interaction

The URL parameters that shall be supported are those defined in Table 6.5.3.3-1 and Table 6.5.3.3-2 describes the request body and possible responses.

Table 6.5.3.3-1: Replace Entity URL parameters Name Data Type Cardinality Remarks type String 0..1 Selection of Entity Types as per clause 4.17.

Table 6.5.3.3-2: Replace Entity request body and possible responses

Request Body

Data Type Cardinality Remarks Entity Fragment 1 Entity Fragment containing a complete representation of the Entity to be replaced.

Response Body

Data Type Cardinality Response Codes Remarks
N/A N/A 204 No content The entity was replaced successfully.
BatchOperationResult 1 207 Multi-Status If the entity input data matches to a
registration, the relevant parts of the
request are forwarded as a distributed
operation.
In the case when an error response is
received back from any distributed
operation, a response body containing
the result returned from each
registration is returned in a
BatchOperationResult structure.

Errors can occur whenever a distributed operation is unsupported, fails or times out, see clause 6.3.17.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an Entity identifier not known to the system, see clause 6.3.2.

PUT /entities/{entityId}

Entity

207 Multi-Status

BatchOperationResult

204 No Content

Possible non-error responses


6.5.3.4 PATCH

This method is bound to the "Merge Entity" operation and shall exhibit the behaviour defined by clause 5.6.17. The Entity identifier is the value of the resource URI variable "entityId". The data to be updated shall be contained in the HTTP request payload body. Figure 6.5.3.4-1 shows the Merge Entity interaction.

Figure 6.5.3.4-1: Merge Entity interaction

The URL parameters that shall be supported are those defined in Table 6.5.3.4-1 and Table 6.5.3.4-2 describes the request body and possible responses.

Table 6.5.3.4-1: Merge Entity URL parameters
Name Data Type Cardinality Remarks
format String 0..1 It shall be one of: "simplified" (or its synonym
"keyValues"). Where present it indicates that a
simplified representation of Entities has been provided
as defined by clause 4.5.4.
In this case, when a merge operation applies to an
existing Attribute the type field of the Attribute shall
remain unchanged (any attempt to modify the type of an
Attribute shall result in a BadRequest error).
lang String 0..1 It represents the natural language of data held in the
request.
When a merge operation applies to a pre-existing
LanguageProperty and the value is supplied as a string
or string array in the payload body, this query parameter
shall be used to determine the key within the
languageMap JSON Object to update.
observedAt String 0..1 It shall be a DateTime (see clause 4.6.3).
When a merge operation applies to a pre-existing
Attribute which previously contained an observedAt sub
attribute, the value held in this query parameter shall be
used if no specific observedAt sub-Attribute is found in
the payload body.

options Comma separated list of strings

0..1 An alternative mechanism to include the format parameter. Deprecated

When its value includes the keyword "simplified" (or its synonym "keyValues"), this indicates that a simplified representation of Entities has been provided as defined by clause 4.5.4. If both format and options are present, the value of the format parameter shall take precedence. type String 0..1 Selection of Entity Types as per clause 4.17.

NGSI-LD Client NGSI-LD System

PATCH /entities/{entityId}

Entity Fragment

207 Multi-Status

BatchOperationResult

204 No Content

Possible non-error responses

Table 6.5.3.4-2: Merge Entity request body and possible responses

| Request Body | Entity Fragment | 1 | | Entity Fragment containing a complete representation of the | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | Attributes to be merged. | | | | |
| | Data Type | Cardinality | | Response Codes | Remarks | | | |
| | N/A | N/A | | 204 No content | All the Attributes were merged | | | |

Data Type Cardinality Remarks

Data Type Cardinality Response Codes Remarks
N/A N/A 204 No content All the Attributes were merged
successfully.
BatchOperationResult 1 207 Multi-Status If the entity input data matches to a
registration, the relevant parts of the
request are forwarded as a distributed
operation.
In the case when an error response is
received back from any distributed
operation, a response body containing
the result returned from each
registration is returned in a
BatchOperationResult structure.
Errors can occur whenever a distributed
operation is unsupported, fails or times
out, see clause 6.3.17.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an Entity identifier not known to the system, see clause 6.3.2.

# Related

* [Operation: Delete Entity](/api-operations/provision/delete-entity.md)
* [Operation: Merge Entity](/api-operations/provision/merge-entity.md)
* [Operation: Replace Entity](/api-operations/provision/replace-entity.md)
* [Operation: Retrieve Entity](/api-operations/consumption/retrieve-entity.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.3.6.8](/framework/architecture/backwards-compatibility-of-context-source-payloads.md)
* [Clause 4.5.23](/framework/data-representation/ngsi-ld-linked-entity-retrieval.md)
* [Clause 4.5.4](/framework/data-representation/simplified-representation.md)
* [Clause 4.5.5](/framework/data-representation/multi-attribute-support.md)
* [Clause 4.6.3](/framework/restrictions/supported-data-types-for-values.md)
* [Clause 5.6.17](/api-operations/provision/merge-entity.md)
* [Clause 5.6.18](/api-operations/provision/replace-entity.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: entities/{entityId}
