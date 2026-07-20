---
type: NGSI-LD Clause
title: "Context Source Identity Information"
description: "5.15.1 Retrieve Context Source Identity Information 5.15.1.1 Description With this operation, a client can obtain Context Source identity information which uniquely defines the Context Source itself."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.15
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.15"
---

5.15.1 Retrieve Context Source Identity Information

5.15.1.1 Description

With this operation, a client can obtain Context Source identity information which uniquely defines the Context Source itself. In the multi-tenancy use case (see clause 4.14), a client can obtain identify information about a specific Tenant within a Context Source.

5.15.1.2 Use case diagram

A client can request the broker to retrieve identity information about a specific Context Source within the NGSI LD system as shown in Figure 5.15.1.2-1.


Figure 5.15.1.2-1: Retrieve Context Source Identity Information

5.15.1.3 Input data

None.

5.15.1.4 Behaviour

- If the Context Source is unable to supply identity information about itself, then error of type NotImplemented shall be raised.
- Return a JSON-LD object representing the identity of the Context Source itself as mandated by clause 5.2.40. This can also include additional configurational data dependent on the specific Context Source implementation.

5.15.1.5 Output data

A JSON-LD object representing the identity of the Context Source as mandated by clause 5.2.40.

# Related

* [Clause 4.14](/framework/languages/supporting-multiple-tenants.md)
* [Clause 5.2.40](/api-operations/data-types/context-source-identity.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.15](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Context Source Identity Information
