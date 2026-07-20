---
type: NGSI-LD Clause
title: "Top-level \"geometry\" field selection algorithm"
description: "A parameter of the request (named geometryProperty) may be used to indicate the name of the GeoProperty to be selected."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.16.1
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.16.1"
---

A parameter of the request (named geometryProperty) may be used to indicate the name of the GeoProperty to be selected. If this parameter is not present, then the default name of "location" shall be used. If the selected GeoProperty has multiple instances as described in clause 4.5.5, either a datasetId shall be specified, in order to define which instance of the value is to be selected, or a default attribute instance exists, which is then selected, if no datasetId was specified. If an entity lacks the GeoProperty as specified or the value does not hold a valid GeoJSON geometry object then the geometry shall be undefined and returned with a value of null - which is syntactically valid GeoJSON.

# Related

* [Clause 4.5.5](/framework/data-representation/multi-attribute-support.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.16.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Top-level "geometry" field selection algorithm
