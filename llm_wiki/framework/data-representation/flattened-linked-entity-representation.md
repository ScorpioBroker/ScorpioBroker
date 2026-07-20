---
type: NGSI-LD Clause
title: "Flattened Linked Entity Representation"
description: "With the flattened representation, the Context Broker response shall always consist of an array of Entities."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.23.3
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.23.3"
---

With the flattened representation, the Context Broker response shall always consist of an array of Entities. This
array will consist of both Linking Entities and Linked Entities (where the retrieved Linked
Entities defined by an annotated Relationship), are appended to the array. This flattened representation allows for
batch operations (see clauses 5.6.7, 5.6.8, 5.6.9 and 5.6.10) be applied directly using the response from the Context
Broker.
An example of this representation can be found in annex C, clause C.2.2.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.23.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Flattened Linked Entity Representation
