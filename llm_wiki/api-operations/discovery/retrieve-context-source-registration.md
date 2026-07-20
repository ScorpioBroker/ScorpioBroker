---
type: NGSI-LD Operation
title: "Retrieve Context Source Registration"
description: "5.10.1.1 Description This operation allows retrieving a specific context source registration from an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.10.1
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.10.1"
---

5.10.1.1 Description

This operation allows retrieving a specific context source registration from an NGSI-LD system.

5.10.1.2 Use case diagram

A context consumer or a context provider can retrieve a specific context source registration from an NGSI-LD system as shown in Figure 5.10.1.2-1.

Figure 5.10.1.2-1: Retrieve context source registration use case

5.10.1.3 Input data

Context source registration identifier (id) of the context source registration to be retrieved (target registration).

5.10.1.4 Behaviour

- If the context source registration id (id) is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about the target context source registration, because there is no existing context source registration whose id (URI) is equivalent, then an error of type ResourceNotFound shall be raised.
- Term to URI expansion of Attribute names shall be observed as mandated by clause 5.5.7.
- Otherwise return a JSON-LD object representing the Context Source Registration as mandated by clause 5.2.9.


5.10.1.5 Output data

A JSON-LD object representing the target context source registration as mandated by clause 5.2.9.

# Related

* [HTTP: Resource: csourceRegistrations/{registrationId}](/http-binding/resource-csourceregistrations-registrationid.md)
* [Clause 5.2.9](/api-operations/data-types/csourceregistration.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.10.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Retrieve Context Source Registration
