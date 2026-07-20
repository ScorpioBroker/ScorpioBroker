---
type: NGSI-LD Clause
title: "Temporal Representation of a Property"
description: "The temporal evolution of a Property (for instance, its historical evolution or future predictions) is composed of the sequence of instances of the referred Property during a period of time within its"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.7
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.7"
---

The temporal evolution of a Property (for instance, its historical evolution or future predictions) is composed of the
sequence of instances of the referred Property during a period of time within its lifetime.
The temporal representation of a Property shall be provided as an Array of JSON-LD objects (or a single JSON-LD
object in case only one is present), each one representing an instance of the Property (as mandated by clause 4.5.2) at a
particular point in time, which is recorded as a Temporal Property of the instance (typically observedAt). See example
in annex C, clause C.5.6. In case the Property is deleted, an instance of the Property is recorded with its value set to the
URI "urn:ngsi-ld:null" and the deletedAt Temporal Property set.
Systems should maintain an instanceId for each such Property instance. Without such an instanceId, it is not possible to
selectively modify or delete temporal information via the NGSI-LD API. The consequences of this may be severe in the
case of modification or deletion requests for legal reasons, e.g. GDPR [i.18]. When implementing the NGSI-LD API on
storage systems that do NOT allow modification or deletion, similar problems may be encountered.

# Related

* [Clause 4.5.2](/framework/data-representation/ngsi-ld-property-representations.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.7](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Temporal Representation of a Property
