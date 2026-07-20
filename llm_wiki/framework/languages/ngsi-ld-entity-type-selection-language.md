---
type: NGSI-LD Clause
title: "NGSI-LD Entity Type Selection Language"
description: "The NGSI-LD Entity Type Selection Language shall be supported by implementations."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.17
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.17"
---

The NGSI-LD Entity Type Selection Language shall be supported by implementations. It is intended to select only those Entities that have the specified Entity Type(s), possibly among others. Entity Types are specified as a disjunction of elements, where each element can either directly be an Entity Type or a conjunction of multiple Entity Types. The logical operators are the same as in the NGSI-LD Query Language specified in clause 4.9. As a disjunction of Entity Types can also be seen as a list, and to be compatible with previous versions of the NGSI-LD API, a comma can be used as an alternative representation of the or operator. For logical and grouping parenthesis are needed. EntityTypes = OrEntityType *(orOp OrEntityType) ; OrEntityType|OrEntityType OrEntityType = %x28 EntityType *(andOp EntityType) %x29 ; (EntityType;EntityType) OrEntityType = EntityType ; EntityType andOp = %x3B ; ; orOp = %x7C / %x2C ; | ,

EntityType is either a valid name as specified in clause 4.6.2 or a URI. EXAMPLE 1: Entities of type Building or House: Building|House Alternative Representation: Building,House


EXAMPLE 2: Entities of type Home and Vehicle:
(Home;Vehicle)
EXAMPLE 3: Entities of type (Home and Vehicle) or Motorhome:
(Home;Vehicle)|Motorhome
Alternative Representation:
(Home;Vehicle),Motorhome
NOTE: The special characters ",", ";", "(" and ")" used in the Entity Type Selection Language are allowed
characters in URIs. The use of short names is recommended.

# Related

* [Clause 4.6.2](/framework/restrictions/supported-names.md)
* [Clause 4.9](/framework/languages/ngsi-ld-query-language.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.17](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Entity Type Selection Language
