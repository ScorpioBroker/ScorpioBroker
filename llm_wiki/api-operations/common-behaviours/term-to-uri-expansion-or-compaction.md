---
type: NGSI-LD Clause
title: "Term to URI expansion or compaction"
description: "NGSI-LD API operations allow clients to use short-hand strings as non-qualified names, particularly for Property, Relationship or Type names and VocabProperty values."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.7
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.7"
---

NGSI-LD API operations allow clients to use short-hand strings as non-qualified names, particularly for Property,
Relationship or Type names and VocabProperty values. For instance, an API client can refer to the term
"Vehicle" as a non-qualified type name. When executing API update-related operations, NGSI-LD systems shall
expand terms to URIs, in order to obtain and store Fully Qualified Names. Likewise, when executing query-related
operations, NGSI-LD systems shall compact URIs (Fully Qualified Names) to short terms in order to provide short
hand strings to context consumers.
The term to URI expansion or compaction shall be performed using a @context as described by the JSON-LD
specification [2], section 5.1, and in clause 4.4. In the absence of a user @context, the term expansion or
compaction shall be performed according to clause 5.5.5.
For the avoidance of doubt, the @context used to perform compaction or expansion of terms shall be the one provided
by each API call (or the default @context in its absence), and not any other @context which might have been supplied
previously. For instance, when performing "Query Entity" operations (clause 5.7.2), the @context used to perform URI
expansion and compaction shall be the one provided by the request.


In case of HTTP binding via GET (clause 6.4.3.2) of the "Query Entity" operation, this means using the JSON-LD Link
Header as described by the JSON-LD specification [2], section 6.2. In case of HTTP binding via POST
(clause 6.23.3.1), of the "Query Entity" operation, this means giving the @context either via Link Header or within the
payload body, depending on the Content-Type Header being "application/json" or
"application/ld+json", respectively.
It is important to warn users that updating a @context could lead to behaviour that might be perceived as inconsistent.
If, for instance, a fully qualified name that qualified a given short-hand name is changed, from that moment onwards,
the short-hand name is referencing a different Attribute. This will effectively change the results of queries that use the
given short name, possibly not giving back anymore the expected set of results.
Moreover, this user @context shall not:
- Contain JSON-LD Scoped Contexts (see [2], section 4.1.8).
- Be embedded into NGSI-LD Attributes, i.e. there cannot be parts of the user @context other than at the top level of the NGSI-LD document. Parts of user @context that are not following the two points above should result in an error of type BadRequestData, because JSON-LD Scoped Contexts and nested embedded @context could be used to modify terms defined in the Core @context or to reshape NGSI-LD Elements during the expansion of terms. As the Core @context is protected and cannot be overridden, when performing term to URI expansion or compaction, implementations shall always consider the Core @context as having absolute precedence, regardless of the position of the Core @context in the @context array of elements. Nonetheless, NGSI-LD data providers may use appropriate term prefixing to ensure that a proper term to URI expansion or compaction is performed. At compaction time, in the event that no matching term is found in the current @context, implementations shall render Fully Qualified Names.

EXAMPLE: An entity of type "Vehicle" bound to a certain @context, C, will match a query by "Vehicle" type if and only if the supplied query @context, Q, maps the term "Vehicle" to the same URI as C. Note that the JsonProperty is designed to hold native JSON values which are by definition not available for expansion and compaction via an @context. Therefore, the given short-hand name is always used for accessing JSON keys within a JsonProperty json element.

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 5.5.5](/api-operations/common-behaviours/default-at-context-assignment.md)
* [Clause 5.7.2](/api-operations/consumption/query-entities.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.7](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Term to URI expansion or compaction
