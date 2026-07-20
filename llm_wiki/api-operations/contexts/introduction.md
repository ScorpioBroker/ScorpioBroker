---
type: NGSI-LD Operation
title: "Introduction"
description: "Context Brokers optionally (see clause 4.3.5) offer the capability to store and serve @contexts to clients."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.13.1
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.13.1"
---

Context Brokers optionally (see clause 4.3.5) offer the capability to store and serve @contexts to clients. The stored @contexts may be managed by clients directly, via the APIs specified in clause 5.13. Clients can store custom user @contexts at the Context Broker, effectively using the Context Broker as a @context server. Moreover, in order to optimize performance, brokers may automatically store and use the stored copies of common @contexts as a local cache, downloading them just once, thus avoiding fetching them over and over again at each NGSI-LD request. In order for the broker to understand if a needed @context is already in the local storage or not, the broker uses the URL, where the @context is originally hosted, as an identifier for it in the local storage. Consequently, the broker has no ability to cache @contexts that arrive to it as embedded parts within the NGSI-LD documents, since they are not uniquely (and implicitly) identified by any URL; Context Brokers only cache @contexts that are referred to by means of explicit URLs (either in the HTTP Link header or as URLs in the payload body). Thus, the recommended best-practice, in order to exploit caching, is that clients do not embed their user @contexts into their NGSI-LD documents; instead clients should explicitly host their user @contexts at their premises, or use the broker's capability to host user @contexts on their behalf.


When an external @context is stored, either explicitly upon a client's request or implicitly downloaded for caching purposes, the Context Broker generates a unique local @context identifier. The original @context's URL, if any, is stored alongside the generated local id. The local id is then used for subsequent managing operations on the specific @context, that are specified in clauses 5.13.2 to 5.13.5. Moreover, the broker tags the entry with the current timestamp, (createdAt) so that, subsequently, clients can check the timestamp before deciding whether to force a refresh of the stored copy of the @context. This is primarily intended as a means for clients to well-behave, thus avoiding triggering continuous refresh of a stored @context on the broker, for fear that it is not at the latest version. Stored @contexts are flagged as one of three kinds: "Cached", "Hosted", "ImplicitlyCreated":

- Cached: @contexts implicitly and automatically fetched by the broker from external URLs during normal NGSI-LD operations are flagged as "Cached". A locally unique identifier is generated for each @context not already in the internal storage. The downloaded content, its URL and the current time in UTC are stored alongside the locally unique identifier. Implementations shall periodically invalidate the "Cached" @contexts. Depending on the binding of the NGSI-LD API to a specific protocol, that specific protocol may provide explicit indications about expiration times of cached content. In such cases, implementations shall comply with the indications provided by the protocol. Implementations should assign a heuristic expiration time when an explicit time is not specified. Entries flagged as "Cached" shall not be served by brokers on-demand, but only be used as a local cache to improve performance.
- Hosted: @contexts that are explicitly added by users are flagged as "Hosted". These entries shall be served by brokers on-demand.
- ImplicitlyCreated: @contexts that are implicitly, but ex-novo, created by the broker as a result of a user request are flagged as "ImplicitlyCreated". For instance, when a client creates a subscription using an @context that is an array, and the broker has to notify with Content-Type "application/json", then the broker needs this @context array to be hosted at a URL. Hence the broker has to create a new @context that is an array, and it is going to be served from an own URL. These entries shall be served by brokers on-demand.

# Related

* [Clause 4.3.5](/framework/architecture/ngsi-ld-api-structure-and-implementation-options.md)
* [Clause 5.13](/api-operations/contexts/storing-managing-and-serving-at-contexts.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.13.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Introduction
