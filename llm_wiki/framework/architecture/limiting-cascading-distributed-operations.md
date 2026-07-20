---
type: NGSI-LD Clause
title: "Limiting Cascading Distributed Operations"
description: "When creating a registration, it is unknown whether the requested data is held at the distributed endpoint, or it is in turn distributed via further registrations."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.3.6.4
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.3.6.4"
---

When creating a registration, it is unknown whether the requested data is held at the distributed endpoint, or it is in turn
distributed via further registrations. It is necessary to include a binding-specific mechanism to request operations only
on the registered endpoint itself to avoid cascades of an excessive lengths, duplicates or loops.
Furthermore, it is not known if any distributed endpoints of a registered Context Source are in turn reliant on
previously encountered Context Sources thus causing an infinite loop. Therefore, when processing a distributed
operation, a specific field listing all previously encountered Context Sources (e.g. a Via header in the response in
case of HTTP binding (IETF RFC 7230 [27])) shall be passed as part of the request and this field can be used to exclude
duplicated sources from matching as context source registrations.
In the case of multi-tenancy (see clause 4.14) each Tenant found within each registered Context Source shall be
considered separately.

# Related

* [Clause 4.14](/framework/languages/supporting-multiple-tenants.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.3.6.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Limiting Cascading Distributed Operations
