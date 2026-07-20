---
type: NGSI-LD Clause
title: "Representation of GeoJSON Geometries in JSON-LD"
description: "There are certain types of GeoJSON geometries, for instance GeoJSON Polygon, whose coordinates are represented using nested array structures (through the coordinates member)."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.7.2
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.7.2"
---

There are certain types of GeoJSON geometries, for instance GeoJSON Polygon, whose coordinates are represented
using nested array structures (through the coordinates member). Such representation may introduce serialization
problems when transforming JSON-LD content into RDF graphs.
Also, when using whole GeoJSON geometries (consisting of type and coordinates) in an NGSI-LD document, its JSON
syntax is only preserved in the regular JSON-LD representation (with separate @context), but not in an expanded
representation. To handle resulting problems, optionally, whole GeoJSON geometries can be represented as a JSON
string.
Implementations shall accept the referred encoded string value, if and only if, it can be parsed into a JSON Object, as
mandated by IETF RFC 8259 [6], meeting the syntax and restrictions mandated by IETF RFC 7946 [8] when
representing a valid Geometry of the type specified.
For the avoidance of doubt, regular encodings of GeoJSON geometries (as JSON Object) shall also be accepted by
implementations, but Context Producers should consider the implications in terms of RDF compatibility.
GeoJSON coordinates shall be considered as consisting of values of a JSON-LD floating point number data type. The
degree of precision of a latitude and longitude (and optional altitude) held within a context broker will depend upon the
implementation. Implementors should note that the GeoJSON specification itself recommends 6 decimal places for
latitude and longitude which equates to roughly 10cm of precision.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.7.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Representation of GeoJSON Geometries in JSON-LD
