---
type: NGSI-LD Clause
title: "Geospatial Properties"
description: "4.7.1 GeoJSON Geometries Geospatial Properties in NGSI-LD shall be represented using GeoJSON Geometries [8]."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.7
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.7"
---

4.7.1 GeoJSON Geometries

Geospatial Properties in NGSI-LD shall be represented using GeoJSON Geometries [8]. With the aim of highlighting
and encoding those Properties which convey geospatial characteristics, NGSI-LD defines a special type of Property
named GeoProperty, defined by the Core NGSI-LD @context described by the present document in clause 4.4.
When dealing with NGSI-LD Entities, implementations shall interpret JSON-LD nodes of type GeoProperty just as
conventional Properties but with the additional requirement that the Value corresponding to such Property shall be a
GeoJSON Geometry. All the Geometries defined by [8] are allowed except GeometryCollection. In addition,
implementations should take the necessary steps to create the corresponding geo-indexes so that information can be
properly returned when geo-queries are executed.
NGSI-LD defines the following Properties of type GeoProperty. Preferably these Properties should be used if they
semantically fit, but if necessary, additional Properties of type GeoProperty can be defined by Context
Producers:
- location is defined as the geospatial Property representing the geographic location of the Entity, e.g. the location of a building or the current location of a car.
- observationSpace is defined as the geospatial Property representing the geographic location that is being observed, e.g. by a sensor. For example, in the case of a camera, the location of the camera and the observation space are different and can be disjoint.
- operationSpace is defined as the geospatial Property representing the geographic location in which an Entity, e.g. an actuator is active. For example, a crane can have a certain operation space. The defined Properties can also be used as part of Context Source Registrations (see clause 5.2.9). In this case they represent locations in which Entities with the respective geospatial Properties are contained. For example, a Context Source that monitors the location of cars in a city may be represented by a Context Source Registration whose Property location corresponds to the space of the city in which the location of cars is monitored.


4.7.2 Representation of GeoJSON Geometries in JSON-LD

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

4.7.3 Concise NGSI-LD GeoProperty

Notwithstanding the restrictions defined in clause 4.5.2.3, an NGSI-LD GeoProperty without additional sub-attributes
shall be represented in a concise but lossless representation by a member whose key is the Property name (a term) and
whose value is the Property Value (see definition of terms in clause 3.1) which itself is also a supported GeoJSON
geometry.
Mandatory
- "type": shall be a supported GeoJSON geometry type as defined in clause 4.7.1.
- "coordinates": shall be present, as defined by the relevant GeoJSON Geometry [8]. When parsing a geospatial value submitted in the concise representation, it shall be possible for the NGSI-LD system to infer the GeoProperty type. Error handing of the payload is left ambiguous if the NGSI-LD system is unable to distinguish a payload as either a Property or a GeoProperty. Furthermore, an NGSI-LD GeoProperty which includes additional Properties or Relationships shall be treated in the same manner as an ordinary NGSI-LD Property (see clause 4.5.2.3) with the exception that if the Property value resolves to a supported GeoJSON geometry, the type "GeoProperty" shall be inferred.

# Related

* [Clause 3.1](/overview/terms.md)
* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.2.3](/framework/data-representation/concise-ngsi-ld-property.md)
* [Clause 4.7.1](/framework/geo-temporal/geojson-geometries.md)
* [Clause 5.2.9](/api-operations/data-types/csourceregistration.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.7](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Geospatial Properties
