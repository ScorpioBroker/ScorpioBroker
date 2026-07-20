---
type: NGSI-LD Clause
title: "Temporal Representation of a Relationship"
description: "The temporal evolution of a Relationship (for instance, its historical evolution or future predictions) is composed of the sequence of instances of the referred Relationship during a period of time wi"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.8
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.8"
---

The temporal evolution of a Relationship (for instance, its historical evolution or future predictions) is composed of the sequence of instances of the referred Relationship during a period of time within its lifetime. The temporal representation of an NGSI-LD Relationship shall be provided as an Array of JSON-LD objects (or a single JSON-LD object in case only one is present), each one representing an instance of the Relationship (as mandated by clause 4.5.3) at a particular point in time, which is recorded as a Temporal Property of the instance (typically observedAt). See example in annex C, clause C.5.5. In case the Relationship is deleted, an instance of the Relationship is recorded with its object set to the URI "urn:ngsi-ld:null" and the deletedAt Temporal Property set. Systems should maintain an instanceId for each such Relationship instance. Without such an instanceId, it is not possible to selectively modify or delete temporal information via the NGSI-LD API. The consequences of this may be severe in the case of modification or deletion requests for legal reasons, e.g. GDPR [i.18]. When implementing the NGSI-LD API on storage systems that do NOT allow modification or deletion, similar problems may be encountered.

# Related

* [Clause 4.5.3](/framework/data-representation/ngsi-ld-relationship-representations.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.8](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Temporal Representation of a Relationship
