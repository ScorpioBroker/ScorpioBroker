---
type: NGSI-LD Clause
title: "Common Behaviours"
description: "5.5.1 Introduction This clause defines common behaviours for the API operations."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5"
---

5.5.1 Introduction

This clause defines common behaviours for the API operations.

When comparing URIs, implementations shall follow the recommendations of IETF RFC 3986 [5], section 6.

5.5.2 Error types

Table 5.5.2-1 details a list of error types defined by NGSI-LD. The particular conditions under which error type shall be raised are defined when describing each operation supported by the API.


Table 5.5.2-1: Error types in NGSI-LD
Error Type Description
https://uri.etsi.org/ngsi-ld/errors/AlreadyExists The referred element already exists.
https://uri.etsi.org/ngsi-ld/errors/BadRequestData The request includes input data which does not meet the
requirements of the operation.
https://uri.etsi.org/ngsi-ld/errors/Conflict The operation conflicts with the current state of the system.
https://uri.etsi.org/ngsi-ld/errors/InternalError There has been an error during the operation execution.
https://uri.etsi.org/ngsi-ld/errors/InvalidRequest The request associated to the operation is syntactically
invalid or includes wrong content.
https://uri.etsi.org/ngsi-ld/errors/LdContextNotAvailable A remote JSON-LD @context referenced in a request cannot
be retrieved by the NGSI-LD Broker and expansion or
compaction cannot be performed.
https://uri.etsi.org/ngsi-ld/errors/NoMultiTenantSupport The NGSI-LD API implementation does not support multiple
tenants.
https://uri.etsi.org/ngsi-ld/errors/NonexistentTenant The addressed tenant does not exist.
https://uri.etsi.org/ngsi-ld/errors/OperationNotSupported The operation is not supported.
https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound The referred resource has not been found.
https://uri.etsi.org/ngsi-ld/errors/TooComplexQuery The query associated to the operation is too complex and
cannot be resolved.
https://uri.etsi.org/ngsi-ld/errors/TooManyResults The query associated to the operation is producing so many
results that can exhaust client or server resources. It should
be made more restrictive.

5.5.3 Error response payload body

When reporting errors back to clients, NGSI-LD implementations shall generate a JSON object in accordance with IETF RFC 7807 [10], section 3.1, including, at least the following terms:

- type: Error type as per clause 5.5.2.

- title: Error title which shall be a short string summarizing the error.

- detail: A detailed message that should convey enough information about the error. Even though IETF RFC 7807 [10] defines a specific MIME type for error payloads, NGSI-LD implementations shall use the standard JSON MIME type ("application/json") when reporting errors, so that old clients or existing tools are not broken.

EXAMPLE:

{

"type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
"title": "Resource not found.",
"detail": "urn:ngsi-ld:Device:widget001 was not found",
"status": 404,
"instance": "urn:ngsi-ld:Device:widget001"

}

5.5.4 General NGSI-LD validation

All the operations that take a JSON-LD document as input shall process such JSON-LD document as follows:

- If the request payload body is not a valid JSON document then an error of type InvalidRequest shall be raised.

- If the data included by the JSON-LD document is not syntactically correct, according to the @context or the API data type definitions, then an error of type BadRequestData shall be raised.

- A Context Producer may supply an additional hint regarding the overall validity of the payload body and the version of the NGSI-LD specification that the API datatype definitions conform to. When receiving such an annotated context production request, a Context Broker that it is only partially capable of interpreting the datatypes held within the payload body may use this information to amend the data held within the payload through applying fallbacks (see clause 4.3.6.8) prior to validation.


- Any attempt to use "urn:ngsi-ld:null" as a first level member value (" ": "urn:ngsi ld:null"), with the exception of NGSI-LD Fragments (see clause 5.4) used in partial update and merge operations (as mandated by clause 5.5.8 and clause 5.5.12) or to represent deleted Properties in concise representation as part of notifications, shall result in an error of type BadRequestData.
- Any attempt to use "urn:ngsi-ld:null" as the right-hand side of value in a Property, as the right-hand side of object in a Relationship or to use {"@none": "urn:ngsi-ld:null"} as the right-hand side of languageMap, with the exception of NGSI-LD Fragments (see clause 5.4) used in update and merge operations (as mandated by clauses 5.5.8 and 5.5.12) and the representation of deleted Properties, Relationships or Language Properties in notifications and the temporal evolution, shall result in an error of type BadRequestData.
- Any attempt to use "urn:ngsi-ld:null" as the value of a key value pair within a JSON object, which is the right-hand side of the value of a Property, with the exception of NGSI-LD Fragments used in merge operations (see clause 5.5.12), shall result in an error of type BadRequestData.

5.5.5 Default @context assignment

If the input provided by an API client does not include any @context, then the implementation shall at minimum assign the Core @context to such an input. In addition, the Context Broker implementation may allow configuring a default user @context (with default terms), to be used when no user @context is provided. The Core @context shall always take precedence.

5.5.6 Operation execution and generic error handling

When executing an operation if an unexpected error happens and the operation cannot be completed, implementations
shall raise an error of type InternalError. This includes, as well, situations such as database timeouts, etc.
If the NGSI-LD endpoint is not capable of executing the requested operation, an error of type OperationNotSupported
shall be raised. This may happen in a distributed architecture where a Context Broker might not be able to store
Entities (only to forward queries to Context Sources), and as a result, certain operations such as "Create Entity"
might not be supported.
When a query operation is so complex that cannot be resolved by an NGSI-LD system, implementations shall raise an
error of type TooComplexQuery.
When a query operation is producing so many results that can potentially exhaust client or server resources, or it can be
just impractical to be managed, implementations shall raise an error of type TooManyResults. The threshold conditions
used as criteria to raise such error is up to each implementation.
When a remote JSON-LD @context referenced by an incoming request is not available, implementations shall raise an
error of type LdContextNotAvailable. If the remote JSON-LD @context is invalid, implementations shall raise an error
of type BadRequestData.

5.5.7 Term to URI expansion or compaction

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

5.5.8 Partial Update Patch Behaviour

The Partial Update Patch procedure modifies an existing NGSI-LD element by overwriting the data at the Attribute
level, replacing it with the data provided in the NGSI-LD Fragment.
When updating NGSI-LD elements (Entities, Context Source Registrations or Context Subscriptions) using
NGSI-LD Fragments, implementations shall determine the exact set of changes being requested by comparing the
content of the provided Fragment (patch) against the current content (a JSON-LD object) of the target element.
With respect to update operations, implementations shall perform an algorithm equivalent to the one described below
(adapted from IETF RFC 7396 [16]), in order to observe the name to URI expansion rules and the JSON-LD null
processing):
- For each member of the Fragment perform the term to URI expansion.
- If the provided Fragment (a JSON Merge Patch document) contains members that do not appear within the target (their URIs do not match), those members are added to the target.
- For each member of the Fragment contained by the target, the target member value is replaced by the value given in the Fragment. In the case of a member representing a reified Property or Relationship including a datasetId, such member is only replaced if the datasetId is the same, otherwise the member of the Fragment is added as a new instance to the target. If no datasetId is present, the default Attribute instance is targeted and replaced if present and otherwise added. In case of a member type (of an entity) in Entity Fragments, all included Entity Types are added, if they are not already contained in the type member of the target.


- For each member of the Fragment, whose value is an NGSI-LD Null, contained by the target, the target member is deleted. In the case of deleting a specific Attribute instance with a datasetId, the handling shall be in accordance with the description found in clause 5.6.5. A datasetId cannot be deleted by setting it to the

value "urn:ngsi-ld:null".

EXAMPLE 1: Given an Entity containing the following Property:

{

"temperature": { "type": "Property", "value": 25, "unitCode": "CEL" "observedAt": "2022-03-14T01:59:26.535Z"

} }

Applying partial attribute update operation (as defined in clause 5.6.4) at the Attribute level onto the temperature Attribute, with the following Attribute Fragment payload:

{

"type": "Property",
"value": 100,
"observedAt": "2022-03-14T13:00:00.000Z"

}

Results in an overwrite of the value and observedAt sub-Attributes, leaving the unitCode sub-Attribute untouched as shown:

{

"temperature": { "type": "Property", "value": 100, "unitCode": "CEL" "observedAt": "2022-03-14T13:00:00.000Z"

} }

EXAMPLE 2: Given an Entity containing the following Property:

{

"temperature": { "type": "Property", "value": 25, "unitCode": "CEL" "observedAt": "2022-03-14T01:59:26.535Z"

} }

Applying an update attributes operation (as defined in clause 5.6.2) onto the Entity as a whole

with the following Entity Fragment payload:

{

"temperature": { "type": "Property", "value": 100, "observedAt": "2022-03-14T13:00:00.000Z"

} }

Results in an overwrite of the whole temperature Attribute - other Attributes would remain untouched. The result is that the value and observedAt sub-Attributes are updated and the unitCode sub-Attribute is removed as shown:

{

"temperature": { "type": "Property", "value": 100, "observedAt": "2022-03-14T13:00:00.000Z"

} }


EXAMPLE 3: Given an Entity containing the following Property:

{

"temperature": { "type": "Property", "value": 25, "unitCode": "CEL" "observedAt": "2022-03-14T01:59:26.535Z"

}

}
Applying an update attributes operation (as defined in clause 5.6.2) onto the Entity as a whole,
with the following Entity Fragment payload:

{

"temperature": { "type": "Property", "value": "urn:ngsi-ld:null"

} }

Results in the deletion of the whole temperature Attribute - all other Attributes remain untouched.

5.5.9 Pagination Behaviour

5.5.9.1 General Pagination Behaviour

When resolving NGSI-LD Query operations, NGSI-LD Systems shall exhibit the behaviour described by the present clause:

- Let Md be equal to the default maximum number of NGSI-LD Elements to be retrieved by the API during each query pagination iteration, as defined by the NGSI-LD implementation.
- Let Mc be equal to the maximum number of NGSI-LD Elements to be retrieved as requested by the NGSI-LD Client. If Mc is undefined then it shall be equal to Md.
- Let L be the maximum number of NGSI-LD Elements to be retrieved by the API during each query pagination iteration. L shall be equal to Mc.
- During query execution and for each pagination iteration, the query resolution mechanisms of the NGSI-LD System shall ensure that only up to a maximum of L NGSI-LD Elements are retrieved and returned to the NGSI-LD client, i.e. the maximum page size per iteration shall not overpass L. Nonetheless, implementations shall take care of not overpassing a maximum size of response payload body, which, in practice, implies that, under certain circumstances, the number of Elements retrieved per page can be lower than L.
- After the retrieval of each page (containing at most L NGSI-LD Elements) implementations shall check whether there are pending NGSI-LD Elements to be retrieved in the context of the current query. If that is the case, implementations shall flag NGSI-LD Clients of the existence of such NGSI-LD Elements. Ultimately, the flagging mechanisms used shall depend on each API binding but shall be present as mandated by the present clause.
- When flagging the existence of additional NGSI-LD Elements (pages) pending to be retrieved, generally, implementations shall provide NGSI-LD Clients pointers to get access to both the following page of NGSI-LD Elements and the previous one, according to the current pagination iteration.
- The pointer to the previous page of NGSI-LD Elements shall be included for all pagination iterations excepting the first one, as, obviously, there will be no previous NGSI-LD Elements.
- When the last page of NGSI-LD Elements is reached, only the pointer to the previous page shall be provided to NGSI-LD Clients, so that they can detect that no more NGSI-LD Elements are available.
- The pointers to NGSI-LD Elements shall contain all the parameters needed to allow NGSI-LD Clients to retrieve the next and previous page, without further interactions with the API.


While iterating over a set of pages, there might be changes in the target result set, due to additions or removals of NGSI-LD Elements occurring in between. Implementations may detect those situations and may warn NGSI-LD Clients appropriately. During pagination, the same @context shall be used. An attempt to use a different @context should result in an BadRequestData error.

5.5.9.2 Pagination option using limit and offset

The general pagination behaviour described in clause 5.5.9.1 only requires pointers to the following and previous pages, which can be implemented in a completely opaque way. Pagination may also be implemented in a transparent way, giving the Context Consumer more control over the process. In this case, the parameters limit and offset should be used, allowing the Context Consumer to adapt the size of the page using limit and jump to a desired set of elements in the results using offset.

5.5.9.3 Pagination with Entity maps

In the case of queries based on Entity maps, the set of Entities considered for the result is fixed with the initial query creating the Entity map. However, the Entity information itself can be dynamic, so filters shall be rechecked before returning results. In the case of split Entities, the Entities in the Entity map can only be considered as candidate Entities, since, at the time of Entity map creation, the Entities have not been aggregated and thus the filters could not be applied. This can only be done when preparing a page for pagination. Thus, Entities not or no longer fitting the query shall be removed from the Entity map during pagination. Pages shall always be filled to the maximum, as long as Entities are available. When using the previous link, the set of Entities on the previous page may not be the same as before, due to dynamic changes resulting in Entities no longer fulfilling the filter criteria of the query. As a result, the first page when going backward, and the last page, when going forward, may have less than the maximum number of Entities.

5.5.10 Multi-Tenant Behaviour

If a Tenant is specified for an NGSI-LD operation, the operation shall only be applied to information related to the
specified Tenant. If no Tenant is specified, the operation shall apply to the implicitly existing default Tenant. If a
Tenant is explicitly specified, but the system implementing the NGSI-LD API does not support multi-tenancy, an
error of type NoMultiTenantSupport should be raised.
In case an operation applies to a Tenant, this information shall also be provided in the response to the operation. This
also applies to notifications sent as a result of subscriptions (clauses 5.8 and 5.11).
A Tenant is represented in form of a String. How the Tenant is specified for an API operation is protocol binding
specific. How Tenants are created, is implementation-specific.
One implementation option is to support the implicit creation of Tenants. This means that a Tenant is implicitly
created when an NGSI-LD operation for creating information targets a new Tenant; this is the case for:
- Create Entity (clause 5.6.1).
- Batch Entity Creation (clause 5.6.7).
- Create or Update Temporal Evolution of an Entity (clause 5.6.11).
- Create Subscription (clause 5.8.1).
- Register Context Source (clause 5.9.2).
- Create Context Source Registration (clause 5.11.2). All other NGSI-LD operations, e.g. for retrieving, updating, appending or deleting information that target a non-existing Tenant should raise an error of type NonexistentTenant. If the system implementing the NGSI-LD API does not support multiple Tenants, the attempt to register a Context Source with Tenant information in the Context Source Registration should also result in an error of type NoMultiTenantSupport.


5.5.11 More than one instance of the same Entity in an Entity array

5.5.11.0 Foreword

The following operations operate on an array of entities (as input payload):

- Batch Entity Creation (clause 5.6.7).
- Batch Entity Creation or Update (Upsert) (clause 5.6.8).
- Batch Entity Update (clause 5.6.9).
- Batch Entity Delete (clause 5.6.10).
- Batch Entity Merge (clause 5.6.20). It is allowed for such an input Entity array to contain more than one instance of the same entity (those instances have identical ids). In order for such a request to be correctly handled, those instances that have the same id are processed by the Broker in the order they have in the array: the higher the index in the array, the later it will be processed. If the order is altered, the outcome may be altered. All Entities and Attributes in the batch will get the same modifiedAt timestamp, so it makes sense to distinguish them via the observedAt temporal property. Implementations shall treat the entity instances as if they had all arrived in separate requests. The following clauses specify the behaviour in each case.

5.5.11.1 Batch Entity Creation case

The first occurrence of an entity in the input array (the oldest one) is used for the creation of the entity. Any subsequent instance of the same entity is reported as an error (entity already exists) in the response.

5.5.11.2 Batch Entity Creation or Update (Upsert) case

This operation has two modes of operation, with an optional flag to select between the two. The default behaviour is to
replace any already existing entities, while the optional behaviour is to update already existing entities. Non existing
entities are created in both modes.
If the entity does not yet exist, the first occurrence of an entity is used to create the entity, and subsequent instances of
that same entity are used to either replace (default behaviour) or to update (optional behaviour) the entity. These replace
or update operations shall be done in chronological order.
Only the entity resulting from merging all of the entity instances, in the correct order, is maintained in the current state
(as defined in clause 4.3.1). For Temporal Evolution of Entities (as defined in clause 4.3.1), all entity
instances shall be taken into account, in the correct order.

5.5.11.3 Batch Entity Update case

This operation has two modes of operation, with an optional flag to select between the two. The default behaviour is to replace any already existing attributes of the entities, while the optional behaviour is to preserve already existing attributes of the entities. Brokers shall send separate notifications for each individual update, taking throttling into account.

5.5.11.4 Batch Entity Delete case

The Batch Entity Delete operation has as input an array of Entity IDs, for the entities to be deleted. If an Entity ID is replicated in the array, the first occurrence will delete the entity, while subsequent occurrences of the same Entity ID will provoke an error in the response (entity does not exist).


5.5.11.5 Batch Entity Merge case

The Batch Entity Merge operation has as input an array of Entity IDs, for the entities to be merged. If an Entity ID is replicated in the array, these merge operations shall be done in chronological order. Only the entity resulting from merging all of the entity instances, in the correct order, is maintained in the current state (as defined in clause 4.3.1). For Temporal Evolution of Entities (as defined in clause 4.3.1), all entity instances shall be taken into account, in the correct order.

5.5.12 Merge Patch Behaviour

The merge patch procedure modifies an existing NGSI-LD element by applying the set of changes found in an
NGSI-LD Fragment data to the target resource. Unlike the partial update patch behaviour (described in clause 5.5.8),
which replaces the complete element on the first level, e.g. a whole Attribute, the procedure described in this clause
merges the provided information with the existing information up to an arbitrary depth, e.g. including going into JSON
objects representing a Property value.
When merging NGSI-LD Entities using NGSI-LD Fragments, implementations shall determine the exact set of changes
being requested by comparing the content

… (truncated; see full clause in the PDF).

# Related

* [Clause 4.3.1](/framework/architecture/introduction.md)
* [Clause 4.3.6.8](/framework/architecture/backwards-compatibility-of-context-source-payloads.md)
* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 5.11.2](/api-operations/csource-subscription/create-context-source-registration-subscription.md)
* [Clause 5.4](/api-operations/ngsi-ld-fragments.md)
* [Clause 5.5.12](/api-operations/common-behaviours/merge-patch-behaviour.md)
* [Clause 5.5.2](/api-operations/common-behaviours/error-types.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Common Behaviours
