---
type: NGSI-LD Clause
title: "Storing, Managing and Serving @contexts"
description: "5.13.1 Introduction Context Brokers optionally (see clause 4.3.5) offer the capability to store and serve @contexts to clients."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.13
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.13"
---

5.13.1 Introduction

Context Brokers optionally (see clause 4.3.5) offer the capability to store and serve @contexts to clients. The stored @contexts may be managed by clients directly, via the APIs specified in clause 5.13. Clients can store custom user @contexts at the Context Broker, effectively using the Context Broker as a @context server. Moreover, in order to optimize performance, brokers may automatically store and use the stored copies of common @contexts as a local cache, downloading them just once, thus avoiding fetching them over and over again at each NGSI-LD request. In order for the broker to understand if a needed @context is already in the local storage or not, the broker uses the URL, where the @context is originally hosted, as an identifier for it in the local storage. Consequently, the broker has no ability to cache @contexts that arrive to it as embedded parts within the NGSI-LD documents, since they are not uniquely (and implicitly) identified by any URL; Context Brokers only cache @contexts that are referred to by means of explicit URLs (either in the HTTP Link header or as URLs in the payload body). Thus, the recommended best-practice, in order to exploit caching, is that clients do not embed their user @contexts into their NGSI-LD documents; instead clients should explicitly host their user @contexts at their premises, or use the broker's capability to host user @contexts on their behalf.


When an external @context is stored, either explicitly upon a client's request or implicitly downloaded for caching purposes, the Context Broker generates a unique local @context identifier. The original @context's URL, if any, is stored alongside the generated local id. The local id is then used for subsequent managing operations on the specific @context, that are specified in clauses 5.13.2 to 5.13.5. Moreover, the broker tags the entry with the current timestamp, (createdAt) so that, subsequently, clients can check the timestamp before deciding whether to force a refresh of the stored copy of the @context. This is primarily intended as a means for clients to well-behave, thus avoiding triggering continuous refresh of a stored @context on the broker, for fear that it is not at the latest version. Stored @contexts are flagged as one of three kinds: "Cached", "Hosted", "ImplicitlyCreated":

- Cached: @contexts implicitly and automatically fetched by the broker from external URLs during normal NGSI-LD operations are flagged as "Cached". A locally unique identifier is generated for each @context not already in the internal storage. The downloaded content, its URL and the current time in UTC are stored alongside the locally unique identifier. Implementations shall periodically invalidate the "Cached" @contexts. Depending on the binding of the NGSI-LD API to a specific protocol, that specific protocol may provide explicit indications about expiration times of cached content. In such cases, implementations shall comply with the indications provided by the protocol. Implementations should assign a heuristic expiration time when an explicit time is not specified. Entries flagged as "Cached" shall not be served by brokers on-demand, but only be used as a local cache to improve performance.
- Hosted: @contexts that are explicitly added by users are flagged as "Hosted". These entries shall be served by brokers on-demand.
- ImplicitlyCreated: @contexts that are implicitly, but ex-novo, created by the broker as a result of a user request are flagged as "ImplicitlyCreated". For instance, when a client creates a subscription using an @context that is an array, and the broker has to notify with Content-Type "application/json", then the broker needs this @context array to be hosted at a URL. Hence the broker has to create a new @context that is an array, and it is going to be served from an own URL. These entries shall be served by brokers on-demand.

5.13.2 Add @context

5.13.2.1 Description

With this operation, a client can ask the broker to store the full content of a specific @context, by giving it to the broker.

5.13.2.2 Use case diagram

A client can add an @context to be stored within an NGSI-LD system as shown in Figure 5.13.2.2-1.


Figure 5.13.2.2-1: Add @context use case

5.13.2.3 Input data

A JSON object that has a top-level field named @context, i.e. a JSON object representing a JSON-LD "local context". As specified in the JSON-LD specification [2], all extra information located outside of the @context subtree in the referenced object shall be discarded.

5.13.2.4 Behaviour

A new entry is created in the local storage and a locally unique identifier (URI) is generated for it. The JSON object representing the client-supplied @context and the current UTC time are stored alongside the locally unique identifier. That identifier shall be given back as a result in the output data. The entry is flagged as being of kind "Hosted". The behaviour described in clause 5.5.4 about JSON and JSON-LD validation shall be applied in case of invalid @context.

5.13.2.5 Output data

A locally unique URI identifying the @context in the broker's internal storage.

5.13.3 List @contexts

5.13.3.1 Description

With this operation a client can obtain a list of URLs that represent all of the @contexts stored in the local context store of the broker. Each URL can be used to download the corresponding @context, and, in case the @context's kind is "Cached", it shall be the original URL the broker downloaded the @context from. In case a details flag is set to true, the client obtains a list of JSON objects, each representing information (metadata) about an @context currently stored by the broker. Each JSON object contains information about the @context's original URL (if any), its local identifier in the broker's storage, its kind ("Cached", "Hosted" and "ImplicitlyCreated"), its creation timestamp, its expiry date, and additional optional information.

5.13.3.2 Use case diagram

A client can list all @contexts stored within an NGSI-LD system as shown in Figure 5.13.3.2-1.


Figure 5.13.3.2-1: List @contexts use case

5.13.3.3 Input data

- A kind filter indicating the kind of stored @contexts that are to be included in the output list. Currently, possible kinds are "Cached", "Hosted" and "ImplicitlyCreated" (optional).
- A boolean details flag indicating that detailed JSON objects representing metadata about the stored @contexts instead of simple URLs are requested (optional).

5.13.3.4 Behaviour

The broker shall provide a URL or JSON object for each @context currently stored in the internal broker's storage, that match the filter. If no filter is specified, all kinds are included.

5.13.3.5 Output data

A list of URLs, or a list of resulting JSON objects containing the following fields:

- URL;
- localId;
- kind;
- createdAt;
- expiresAt [OPTIONAL];
- lastUsage [OPTIONAL];
- numberOfHits [OPTIONAL, number of times the @context was found in the storage];
- extraInfo [OPTIONAL, used by implementations to report any kind of custom information].


5.13.4 Serve @context

5.13.4.1 Description

With this operation a client can obtain the full content of a specific @context (only for @contexts of kind "Hosted" or "ImplicitlyCreated"), which is currently stored in the broker's internal storage, or its metadata (for all kinds of stored @contexts).

5.13.4.2 Use case diagram

A client can request the broker to serve a specific @context stored within the NGSI-LD system as shown in Figure 5.13.4.2-1.

Figure 5.13.4.2-1: Serve @context use case

5.13.4.3 Input data

- The locally unique identifier that identifies the desired @context in the broker's internal storage. Such unique identifiers are obtained by the client as a result of either a "Add @context" (clause 5.13.2) API operation or of a "List @contexts" (clause 5.13.3) API operation. For @contexts of kind "Cached" this can also be the original URL the broker downloaded the @context from.
- A boolean details flag indicating that a JSON object representing metadata about the @context, instead of the full content, is requested (optional).

5.13.4.4 Behaviour

- If details is set to false, or details is not present, the broker shall give back the full content of the @context that corresponds to the indicated local identifier, serving it from its internal storage, if the @context that corresponds to the indicated local identifier is of kind "Hosted" or "ImplicitlyGenerated". It shall give back OperationNotSupported error if it is of kind "Cached". It shall give back ResourceNotFound if the identifier is not found.
- Otherwise, if details is set to true, the broker shall give back metadata about the @context that corresponds to the indicated local identifier. It shall give back ResourceNotFound error if the identifier is not found.


5.13.4.5 Output data

The full content of the indicated @context (or its metadata as specified in clause 5.13.3.5), or ResourceNotFound/OperationNotSupported errors.

5.13.5 Delete and Reload @context

5.13.5.1 Description

With this operation, a client supplies a local identifier to the broker, indicating a stored @context, that the broker shall remove from its storage. For @contexts of kind "Cached" this can also be the original URL the broker downloaded the @context from. If the entry in the local storage that corresponds to the identifier is itself an array of @contexts, this operation will not delete the children, i.e. the @contexts in the array, but just the entry.

5.13.5.2 Use case diagram

A client can request the broker to delete (and optionally reload) a specific @context stored within the NGSI-LD system as shown in Figure 5.13.5.2-1.

Figure 5.13.5.2-1: Delete and Reload @context use case

5.13.5.3 Input data

- The locally unique identifier that identifies the desired @context in the broker's internal storage. For @contexts of kind "Cached" this can also be the original URL the broker downloaded the @context from.
- A reload boolean flag indicating that reloading of the @context shall be attempted (optional).

5.13.5.4 Behaviour

- If the @context identifier is not supplied, then an error of type BadRequestData shall be raised.
- If the @context identifier does not correspond to any existing entry in the @context storage, then an error of type ResourceNotFound shall be raised.


- If reload is true and the kind of the @context is "Cached", implementations shall try to re-download the identified @context from its original URL, before removing it from the internal storage. If downloading fails, or the downloaded @context is invalid according to JSON and JSON-LD validation of clause 5.5.4, then an error of type LdContextNotAvailable shall be raised. More detailed information about the errors shall be specified in the ProblemDetails (see IETF RFC 7807 [10]) field of the response. In case of any error, the operation ends without removing the existing @context. Otherwise, the existing @context is replaced with the newly downloaded one.
- If reload is true and the kind of the @context is not "Cached", implementations shall return a BadRequestData error.
- If reload is false (or reload is not supplied), implementations shall remove from the internal storage the @context that corresponds to the given identifier. The local identifier is used for finding the @contexts in the internal broker's storage. If the local identifier is not in the storage, a ResourceNotFound error shall be raised.

5.13.5.5 Output data

None.

# Related

* [Clause 4.3.5](/framework/architecture/ngsi-ld-api-structure-and-implementation-options.md)
* [Clause 5.13.2](/api-operations/contexts/add-at-context.md)
* [Clause 5.13.3](/api-operations/contexts/list-at-contexts.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.13](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Storing, Managing and Serving @contexts
