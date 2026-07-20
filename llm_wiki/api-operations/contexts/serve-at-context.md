---
type: NGSI-LD Operation
title: "Serve @context"
description: "5.13.4.1 Description With this operation a client can obtain the full content of a specific @context (only for @contexts of kind \"Hosted\" or \"ImplicitlyCreated\"), which is currently stored in the brok"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.13.4
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.13.4"
---

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

# Related

* [HTTP: Resource: jsonldContexts/{contextId}](/http-binding/resource-jsonldcontexts-contextid.md)
* [Clause 5.13.2](/api-operations/contexts/add-at-context.md)
* [Clause 5.13.3](/api-operations/contexts/list-at-contexts.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.13.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Serve @context
