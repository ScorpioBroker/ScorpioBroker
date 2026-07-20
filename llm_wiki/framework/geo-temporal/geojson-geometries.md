---
type: NGSI-LD Clause
title: "GeoJSON Geometries"
description: "Geospatial Properties in NGSI-LD shall be represented using GeoJSON Geometries [8]."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.7.1
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.7.1"
---

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

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 5.2.9](/api-operations/data-types/csourceregistration.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.7.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — GeoJSON Geometries
