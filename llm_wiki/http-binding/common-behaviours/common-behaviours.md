---
type: NGSI-LD Clause
title: "Common Behaviours"
description: "6.3.1 Introduction This clause extends the API common behaviours to the particularities of the HTTP REST binding."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.3
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.3"
---

6.3.1 Introduction

This clause extends the API common behaviours to the particularities of the HTTP REST binding. For each operation implementations shall exhibit the common behaviours as specified by clause 5.5 and the behaviours defined by the present clause.

6.3.2 Error Types

This clause associates API error types (which shall be contained in the response payload body) defined by clause 5.5.2 with HTTP status codes as shown in Table 6.3.2-1.

Table 6.3.2-1: Mapping of error types to HTTP status codes
Error Type HTTP status
https://uri.etsi.org/ngsi-ld/errors/AlreadyExists 409
https://uri.etsi.org/ngsi-ld/errors/BadRequestData 400
https://uri.etsi.org/ngsi-ld/errors/InternalError 500
https://uri.etsi.org/ngsi-ld/errors/InvalidRequest 400
https://uri.etsi.org/ngsi-ld/errors/LdContextNotAvailable 504
https://uri.etsi.org/ngsi-ld/errors/NoMultiTenantSupport 501
https://uri.etsi.org/ngsi-ld/errors/NonexistentTenant 404
https://uri.etsi.org/ngsi-ld/errors/OperationNotSupported 422
https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound 404
https://uri.etsi.org/ngsi-ld/errors/TooComplexQuery 403
https://uri.etsi.org/ngsi-ld/errors/TooManyResults 403

In addition, implementations shall support the standard specific errors of HTTP bindings, such as the following:

- "Method Not Allowed" (405) which shall be raised when a client invokes a wrong HTTP verb over a resource. Implementations shall provide the allowed HTTP methods as mandated by IETF RFC 7231 [3] in section 6.5.5.
- "Request Entity too large" (413) which shall be raised when the HTTP input data stream provided by a client was too large i.e. too many bytes.
- "Length required" (411) which shall be raised when an HTTP request provided by a client does not define the "Content-Length" HTTP header.
- "Unsupported Media Type" (415) which shall be raised when an HTTP request payload body (as per the "Content-Type" header) it is not "application/json" nor "application/ld+json".
- "Not Acceptable" (406) which shall be raised when the response media types that are acceptable by a client (as per the "Accept" header) do not include or expand to "application/json" nor "application/ld+json".

6.3.3 Reporting errors

When an API operation results in an error, implementations shall return an HTTP response as follows:

- Content-Type: application/json.
- HTTP Status Code: As per clause 6.3.2 depending on error type.
- Payload body: A JSON object including all the terms defined by clause 5.5.3.


6.3.4 HTTP request preconditions

For HTTP POST, PATCH and PUT HTTP requests implementations shall check the following preconditions:

- Content-Type header shall be "application/json" or "application/ld+json".
- Content-Length header shall include the length of the request payload body. For HTTP PATCH requests "application/merge-patch+json" is allowed as Content-Type, as mandated by IETF RFC 7396 [16]. Implementations shall interpret such MIME type as equivalent to "application/json". For HTTP GET requests and for HTTP POST operations corresponding to "Query Entities" (see clause 5.7.2) and "Query Temporal Evolutions of Entities" (see clause 5.7.4), implementations shall check the following preconditions:
- Accept header shall include (or define a media range that can be expanded to) at least one of: - "application/json" - "application/ld+json" - "application/geo+json" The order of the list above is significant. If the Accept header can be expanded to more than one of the options of the list, the first one of the list shall be selected, unless amended by the HTTP Accept header processing rules, e.g. the presence of a "q" parameter indicating a relative weight, (as mandated by IETF RFC 7231 [3], section 5.3.2) require otherwise. If the Accept header is not present, "application/json" shall be assumed. If an incoming HTTP request does not meet the preconditions stated above, an HTTP error response of type InvalidRequest shall be returned, with the following exceptions:
- "Content-Length" HTTP header absence, shall result in just a 411 HTTP status code (without any payload body).
- Unsupported Media Type, i.e. "Content-Type" header is not "application/json" nor "application/ld+json", shall result in just a 415 HTTP status code (without any payload body).
- Not Acceptable Media Type, i.e. "Accept" header does not imply "application/json" nor "application/ld+json", shall result in a 406 HTTP status code and the body of the message shall contain the list of the available representations of the resources. Notwithstanding the above, if the Accept Header is set to "application/geo+json":
- For Context Information Consumption operations only, specifically "Retrieve Entity" (see clause 5.7.1) and "Query Entity" (clause 5.7.2) GeoJSON is considered as an acceptable content type and a GeoJSON payload will be returned.
- For all other operations, the request will result in a Not Acceptable Media Type error, returning a 406 HTTP status code and the body of the message shall contain the list of the available representations of the resources.

6.3.5 JSON-LD @context resolution

In the HTTP REST binding, implementations shall resolve the JSON-LD @context associated to an incoming HTTP request as follows:

- If the request verb is GET or DELETE, then the associated JSON-LD @context shall be obtained from a Link header [7] as mandated by JSON-LD [2], section 6.2. In the absence of such Link header, then the associated @context shall be the default JSON-LD @context.


EXAMPLE: The structure of the referred Link header is shown below. The first component (between < >) is a dereferenceable URI pointing to the JSON-LD document which contains the @context to be used to expand the terms used by the corresponding operation. The second parameter is a fixed, non dereferenceable URI used to denote a unique identifier and semantics for this header (marking it as a link to a JSON-LD @context). The third and final parameter flags the MIME type of the linked resource (JSON-LD). Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json".

- If the request verb is POST, PATCH or PUT and the Content-Type header is "application/json", then the @context shall be obtained from a Link Header as mandated by JSON-LD [2], section 6.2. In the absence of such Link Header, then the @context shall be the default @context. In any case, if the request payload body (as JSON) contains a @context term, then an HTTP error response of type BadRequestData shall be raised.
- If the request verb is POST, PATCH or PUT and the Content-Type header is "application/ld+json", then the associated @context shall be obtained from the request payload body itself. If no @context can be obtained from the request payload body, then an HTTP error response of type BadRequestData shall be raised. In any case, the presence of a JSON-LD Link header in the incoming HTTP request when the Content-Type header is "application/ld+json" shall result in an HTTP error response of type BadRequestData. In summary, from a developer's perspective, for POST, PATCH and PUT operations, if MIME type is "application/ld+json", then the associated @context shall be provided only as part of the request payload body. Likewise, if MIME type is "application/json", then the associated @context shall be provided only by using the JSON-LD Link header. No mixes are allowed, i.e. mixing options shall result in HTTP response errors. Implementations should provide descriptive error messages when these situations arise. In contrast, GET and DELETE operations always take their input @context from the JSON-LD Link Header.

6.3.6 HTTP response common requirements

Implementations shall honour the Accept header provided by HTTP requests as mandated by clause 6.3.4:

- If the target response's MIME type is "application/json" such response shall include a Link to the associated JSON-LD @context as mandated by [2], section 6.2. - If the Prefer header [26] is set to "ngsi-ld= " then implementations shall set the Preference-Applied header to "ngsi-ld= " in the returned response indicating which version of the NGSI-LD specification the payload body conforms to, as mandated in clause 4.3.6.8.
- If the target response's MIME type is "application/ld+json", then the response payload body provided by the HTTP response shall include a JSON-LD @context. - If the Prefer Header [26] is set to "ngsi-ld= " then implementations shall set Preference Applied header to "ngsi-ld= " in the returned response indicating which version of the NGSI-LD specification the payload body conforms to, as mandated in clause 4.3.6.8.
- If the target response's MIME type is "application/geo+json" and the Prefer Header [26] is omitted or set to "body=ld+json", then the response payload body provided by the HTTP response shall include a JSON-LD @context, and the representation of the entities shall be in GeoJSON format in the response payload body.
- If the target response's MIME type is "application/geo+json" and the Prefer Header [26] is set to "body=json" such response shall include a Link to the associated JSON-LD @context as mandated by [2], section 6.2 and the representation of the entities shall be in GeoJSON format in the response payload body, and @context shall be omitted from the payload body. Operations where the response payload body is not present such as successful HTTP POST, PATCH, PUT or DELETE operations and all error responses, do not include the Link header in the response.


Operations that result in an error that return a payload or that result in a partial success (207 Multi-Status) shall always
respond with MIME type "application/json", regardless of the Accept header. It is assumed that if a client
application understands any of the supported MIME types, the application shall understand "application/json"
errors. Only Fully Qualified Names shall be used in the payload body of error or partial success responses, as there is no
context present.
No Content-Length HTTP header shall be present if the response code is 204.

6.3.7 Representation of Entities

For HTTP GET and POST operations corresponding to "Retrieve Entity" (see clause 5.7.1) and "Query Entities" (see
clause 5.7.2), Context Broker implementations shall support the parameter specified in Table 6.3.7-1, which
specifies all possible supported representations formats.
In contrast, at a minimum, registered Context Source implementations shall support the normalized representation
of Entities as default. When a registered Context Source is unable to support additional representations, a 501 Not
Implemented Error shall be raised.


Table 6.3.7-1: Entity representations: format + options parameter
Name Data Type Cardinality Remarks
format String 0..1 When its value is "normalized", a normalized
representation of Entities shall be provided as defined by
clause 4.5.1, with Attributes returned in the normalized
representation as defined in clauses 4.5.2.2, 4.5.3.2,

# Related

* [Clause 4.3.6.8](/framework/architecture/backwards-compatibility-of-context-source-payloads.md)
* [Clause 4.5.1](/framework/data-representation/ngsi-ld-entity-representation.md)
* [Clause 5.5](/api-operations/common-behaviours/common-behaviours.md)
* [Clause 5.5.2](/api-operations/common-behaviours/error-types.md)
* [Clause 5.5.3](/api-operations/common-behaviours/error-response-payload-body.md)
* [Clause 5.7.1](/api-operations/consumption/retrieve-entity.md)
* [Clause 5.7.2](/api-operations/consumption/query-entities.md)
* [Clause 5.7.4](/api-operations/consumption/query-temporal-evolution-of-entities.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Common Behaviours
