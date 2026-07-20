---
type: NGSI-LD Clause
title: "Entity Ordering"
description: "4.23.1 Introduction When a Context Consumer queries for Entities retrieved from a single context broker, it shall be possible to request that the array of Entities returned are ordered according to th"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.23
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.23"
---

4.23.1 Introduction

When a Context Consumer queries for Entities retrieved from a single context broker, it shall be possible to request that the array of Entities returned are ordered according to the values held within an Entity Attribute. Context Broker implementations shall interpret such requests and return the array of Entities sorted accordingly. For each Attribute name listed, one of four types of sort order can be specified:

- Sort in ascending order by target value or target object
- Sort in descending order by target value or target object
- Sort by distance in ascending order from a specified GeoJSON geometry
- Sort by distance in descending order from a specified GeoJSON geometry When sorting by ascending or descending order, the preferred sort order for numbers and datetimes is trivial, however for strings the default collation order shall be defined as using ICU "root" collation order ("und-x-icu") which returns a reasonable language-agnostic sort order (see IETF RFC 6067 [36]). Sort by distance shall be limited to GeoProperties, and shall be defined as calculating spherical distance using the Haversine formula. Sort ordering is never applied to distributed operations, and the defined sort ordering strategy may depend on implementation specific configurations.

4.23.2 Datatype Comparison Order

When sorting by value, and the values of Attributes are of mixed datatypes, the following comparison order (null values last), shall be implicitly applied:

- Numbers
- Strings
- Object
- Array


- Boolean
- Time
- Date
- DateTime
- Null
- Attribute does not exist Additionally, when sorting by distance, the following comparison order, shall be applied:
- Attribute is a GeoProperty
- Attribute is not a GeoProperty - sorted by value as shown above.

4.23.3 NGSI-LD Entity Ordering Language

The NGSI-LD Entity Ordering Language shall be supported by implementations for the order parameter (i.e. orderBy). Its aim is to specify the Attributes to be used when ordering an Array of Entities retrieved. Ordering Attributes are specified as a disjunction of elements, where each element can either directly be an Attribute name, or an Attribute name followed by a direction (either ascending or descending, where ascending shall be the default if not supplied). The comma character shall be used to separate the elements to be used for ordering which shall be applied sequentially. In the following, ABNF grammar for NGSI-LD Entity Ordering Language is given.

thenOp = %x2C ; , directionOp ::= asc | desc | dist-asc | dist-desc OrderingTerm = AttrName *1(DirectionTerm) *(thenOp OrderingTerm) DirectionTerm = %x3B directionOp. ; ;

See clause 4.9 for the definition of AttrName.
EXAMPLE 1: ?orderBy=name - applies sort ordering to rank Entities in ascending order using the value of
the name Attribute.
EXAMPLE 2: ?orderBy=name,age - applies sort ordering to rank Entities in ascending order using the
value of the name Attribute and thereafter where multiple Entities have the same name, by
ascending order using the age Attribute.
EXAMPLE 3: ?orderBy=name,age;desc - applies sort ordering to rank Entities in ascending order using
the value of the name Attribute and thereafter where multiple Entities have the same name, by
descending order using the value of age Attribute.
EXAMPLE 4: ?orderBy=address[city] - applies sort ordering using a sub-item within a Property Value
defined as a complex JSON object. The trailing path is [city]. It is used to refer to a particular
subitem within the value of the address Property.
EXAMPLE 5: ?orderBy=name.observedAt - applies sort ordering based on an attribute path that is
defined by the production rule Attribute, as a dot-separated list of names. Such a list is intended to
address a Property or Relationship included by the matching entities subjacent graph.
When sorting data using a specific ICU collation order as defined by IETF RFC 6067 [36], the collation element
(parameter name collation) shall represent the preferred ordering.
EXAMPLE 6: ?orderBy=name&collation=und-u-ks-identic - applies sort ordering to rank Entities in
ascending order using the case insensitive value of the name Attribute.

EXAMPLE 7: ?orderBy=name&collation=de-u-co-phonebk - applies sort ordering to rank Entities in ascending order using the value of the name Attribute and sorted according to German phonebook collation ordering.


For sort by distance, the coordinates element (parameter name orderFrom) shall represent the coordinates of the
reference geometry as mandated by IETF RFC 7946 [8], section 3.1.2.
EXAMPLE 8: ?orderBy=location;dist-asc&orderFrom=[8,40] - applies sort ordering to rank
Entities in ascending distance from a Point with respect to the location GeoProperty.
EXAMPLE 9: ?orderBy=location;dist-desc&orderFrom=[8,40] - applies sort ordering to rank
Entities in descending distance from a Point with respect to the location GeoProperty.
EXAMPLE 10: ?orderBy=location;dist-asc
&orderFrom=[[8,40],[9,42],[9,45],[8,40]]
&orderGeometry=LineString
applies sort ordering to rank Entities in ascending distance from a LineString with respect to the
location GeoProperty.

# Related

* [Clause 4.9](/framework/languages/ngsi-ld-query-language.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.23](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Entity Ordering
