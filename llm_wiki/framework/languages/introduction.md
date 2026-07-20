---
type: NGSI-LD Clause
title: "Introduction"
description: "When a Context Consumer queries for Entities retrieved from a single context broker, it shall be possible to request that the array of Entities returned are ordered according to the values held within"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.23.1
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.23.1"
---

When a Context Consumer queries for Entities retrieved from a single context broker, it shall be possible to request that the array of Entities returned are ordered according to the values held within an Entity Attribute. Context Broker implementations shall interpret such requests and return the array of Entities sorted accordingly. For each Attribute name listed, one of four types of sort order can be specified:

- Sort in ascending order by target value or target object
- Sort in descending order by target value or target object
- Sort by distance in ascending order from a specified GeoJSON geometry
- Sort by distance in descending order from a specified GeoJSON geometry When sorting by ascending or descending order, the preferred sort order for numbers and datetimes is trivial, however for strings the default collation order shall be defined as using ICU "root" collation order ("und-x-icu") which returns a reasonable language-agnostic sort order (see IETF RFC 6067 [36]). Sort by distance shall be limited to GeoProperties, and shall be defined as calculating spherical distance using the Haversine formula. Sort ordering is never applied to distributed operations, and the defined sort ordering strategy may depend on implementation specific configurations.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.23.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Introduction
