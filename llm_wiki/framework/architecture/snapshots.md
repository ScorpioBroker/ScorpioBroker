---
type: NGSI-LD Clause
title: "Snapshots"
description: "Context information can be dynamic, e.g."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.3.7
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.3.7"
---

Context information can be dynamic, e.g. when a query result contains many Entities, and the user has to paginate through the result set, the values encountered later may be from a much later point in time than earlier results, making them incomparable. To get more consistent results, the Snapshot concept is introduced, which can be optionally implemented by Context Brokers. It enables "freezing" the result by retrieving all results immediately and persisting them locally. Context Consumers can then query and analyse the Snapshot in a much more consistent way. This is especially relevant for distributed cases, where information initially was distributed across multiple Context Brokers and Context Sources. Snapshots offer the full functionality of NGSI-LD available locally and can be used as a basis for simulations that enable making predictions or "what-if" analyses, which are often needed for digital twins.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.3.7](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Snapshots
