---
type: NGSI-LD Clause
title: "Concise NGSI-LD LanguageProperty"
description: "Broker will return data in the most concise lossless representation possible, for example removing all Attribute type members."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.18.3
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.18.3"
---

Broker will return data in the most concise lossless
representation possible, for example removing all Attribute
type members.
When its value is "simplified" (or its synonym
"keyValues"). a simplified representation of Entities
shall be provided as defined by clause 4.5.4.
If the Accept Header is set to
"application/geo+json" the response will be in
simplified GeoJSON format as defined by clause 4.5.17.
options Comma separated list of strings 0..1 An alternative mechanism to include the format parameter.
Deprecated
When its value includes the keyword "normalized", a
normalized representation of Entities shall be provided as
defined by clause 4.5.1, with Attributes returned in the
normalized representation as defined in clauses 4.5.2.2,
4.5.3.2, 4.5.18.2 and 4.5.20.2.
When its value includes the keyword "concise", a
concise lossless representation of Entities shall be
provided as defined by clause 4.5.1. with Attributes
returned in the concise representation as defined in
clauses 4.5.2.3, 4.5.3.3, 4.5.18.3 and 4.5.20.3. In this case
the Context Broker will return data in the most
concise lossless representation possible, for example
removing all Attribute type members.
When its value includes the keyword "simplified" (or
its synonym "keyValues"). a simplified representation
of Entities shall be provided as defined by clause 4.5.4.
If the Accept Header is set to
"application/geo+json" the response will be in
simplified GeoJSON format as defined by clause 4.5.17.

# Related

* [Clause 4.5.1](/framework/data-representation/ngsi-ld-entity-representation.md)
* [Clause 4.5.17](/framework/data-representation/simplified-geojson-representation-of-entities.md)
* [Clause 4.5.4](/framework/data-representation/simplified-representation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.18.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Concise NGSI-LD LanguageProperty
