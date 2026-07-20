---
type: NGSI-LD Operation
title: "Delete and Reload @context"
description: "5.13.5.1 Description With this operation, a client supplies a local identifier to the broker, indicating a stored @context, that the broker shall remove from its storage."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.13.5
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.13.5"
---

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

* [HTTP: Resource: jsonldContexts/{contextId}](/http-binding/resource-jsonldcontexts-contextid.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.13.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Delete and Reload @context
