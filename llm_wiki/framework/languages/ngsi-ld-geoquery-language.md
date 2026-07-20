---
type: NGSI-LD Clause
title: "NGSI-LD Geoquery Language"
description: "The NGSI-LD Geoquery language shall be supported by implementations."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.10
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.10"
---

The NGSI-LD Geoquery language shall be supported by implementations. It is intended to define predicates which
allow testing whether a specific topological spatial relationship exists between a pair of geometries: a target geometry
and a reference geometry. The target geometry represents a geospatial Property of an Entity, typically, the location of
the Entity.
A total of four parameters are defined in order to fully specify an NGSI-LD Geoquery:
- georel, to express the desired geospatial relationship;
- geometry, to express the type of the reference geometry;
- coordinates, to express the reference geometry;
- geoproperty, to express the target geometry of an Entity. This parameter is optional, location is the default.


The following grammar defines the syntax for the geospatial relationships (parameter name georel):

andOp = %x3B ; ;
equal = %x3D %x3D ; ==
georel = nearRel / withinRel / containsRel / overlapsRel / intersectsRel / equalsRel / disjointRel
nearRel = nearOp andOp distance equal PositiveNumber ; near;max(min)Distance==x (in meters)
distance = "maxDistance" / "minDistance"
nearOp = "near"
withinRel = "within"
containsRel = "contains"
intersectsRel = "intersects"
equalsRel = "equals"
disjointRel = "disjoint"
overlapsRel = "overlaps"
PositiveNumber shall be a non-zero positive number as mandated by the JSON Specification. Thus, it shall follow the
ABNF Grammar, production rule named Number, section 6 of IETF RFC 8259 [6], excluding the 'minus' symbol and
excluding the number 0.
Reference geometries shall be specified by:
- A geometry type (parameter name geometry) as defined by the GeoJSON specification (IETF RFC 7946 [8], section 1.4), except GeometryCollection.
- A coordinates (parameter name coordinates) element which shall represent the coordinates of the reference geometry as mandated by IETF RFC 7946 [8], section 3.1.1. Target geometry, i.e. the target Entity's GeoProperty to which the geoquery is to be applied, can be specified by an extra parameter named geoproperty. The GeoProperty's name shall be specified as short hand name and not a fully qualified one, because, when the query language is used, an @context properly defining all the terms (as per clause 5.5.7) shall be issued. If no geoproperty is specified, the geoquery is applied to the default Property location (see clause 4.7.1). Note that proper URL encoding shall be used by HTTP binding API clients when using these examples.

EXAMPLE 1: ?georel=near;maxDistance==2000 &geometry=Point &coordinates=[8,40] &geoproperty=observationSpace

EXAMPLE 2: ?georel=within &geometry=Polygon &coordinates=[[[100.0,0.0],[101.0,0.0],[101.0,1.0],[100.0,1.0], [100.0,0.0]]] &geoproperty=location

EXAMPLE 3: A query encoded as an HTTP Query String. Note that this is HTTP binding specific, to be used via GET method, as defined in clause 6.4.3.2. ?georel=near;maxDistance==2000 &geometry=Point &coordinates=[8,40]

The semantics of the different geospatial relationships defined above is as follows, and shall be supported by compliant implementations:

- near statement (production rule named nearRel): - maxDistance modifier. For an entity to match it has to be within the buffer geometric object (as defined by [14]) given by the reference geometry, with distance (in meters) equal to the number conveyed (production rule named PositiveNumber). - minDistance modifier. For an entity to match it has to be disjoint with the buffer geometric object (as defined by [14]) given by the reference geometry, with distance (in meters) equal to the number conveyed (production rule named PositiveNumber).


- equals relationship (production rule named equalsRel). For an entity to match, the target geometry shall be equal, as specified by [14], to the reference geometry.
- disjoint relationship (production rule named disjointRel). For an entity to match, the target geometry shall be disjoint, as specified by [14], to the reference geometry.
- intersects relationship (production rule named intersectsRel). For an entity to match, the target geometry shall intersect, as specified by [14], with the reference geometry.
- within relationship (production rule named withinRel). For an entity to match, the target geometry shall be within, as specified by [14], the reference geometry.
- contains relationship (production rule named containsRel). For an entity to match, the target geometry shall contain, as specified by [14], the reference geometry.
- overlaps relationship (production rule named overlapsRel). For an entity to match, the target geometry shall overlap, as specified by [14], the reference geometry. When resolving geo-queries, Entities which do not convey the target GeoProperty of the query shall be considered as non-matching.

# Related

* [Clause 4.7.1](/framework/geo-temporal/geojson-geometries.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.10](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Geoquery Language
