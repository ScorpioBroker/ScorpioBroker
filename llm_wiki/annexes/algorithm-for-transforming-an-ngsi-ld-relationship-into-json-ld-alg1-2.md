---
type: Reference
title: "Algorithm for transforming an NGSI-LD Relationship into JSON-LD (ALG1.2)"
description: "Let Rs be the Relationship that has to be transformed."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-D.4
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "D.4"
---

Let Rs be the Relationship that has to be transformed. It is defined by (R, "AliasR", Robj), where R denotes a
Relationship Type Id, "AliasR" is the Relationship's name and Robj is the identifier of the target object of the
Relationship.
Rs might be associated to extra Properties or Relationships.
Let O be the output JSON-LD object and C the current JSON-LD context:
1) Execute the following statements:
a) If no member with "AliasR" is present in O, add a new member to O with key "AliasR" and value an
object structure, let it be named Or, and defined as in the following. Otherwise, add all existing members
with "AliasR" to a JSON-LD array and in addition put the object structure Or as defined in the following:
- <"object", Robj>.
- <"type", "Relationship">.
b) For each Property associated to Rs (Pss) run the algorithm ALG1.1 taking the following inputs:
- Ps → Pss.
- O → Or.
- C → C.
c) For each Relationship associated to Rs (Rss) recursively run the present algorithm ALG1.2 taking the
following inputs:
- Rs → Rss.
- O → Or.
- C → C.
2) Return (O,C) and end of the algorithm.


Annex E (informative): RDF-compatible specification of NGSI-LD meta-model

The content of this annex is now in ETSI GS CIM 006 [i.8].


Annex F (informative): Conventions and syntax guidelines

When new terms are defined, they are marked in bold, and terms are capitalized thereafter.
EXAMPLE 1: NGSI-LD Linked Entity, Linked Entity.
API Parameter names are always in lowercase.
EXAMPLE 2: options.
Entity Types are defined using lowercase but with a starting capital letter.
EXAMPLE 3: Vehicle, Building, ParkingSpace.
JSON-LD nodes and terms are always defined using camel case notation starting with lower case.
EXAMPLE 4: createdAt, value, unitCode.
When referring to special terms, data types or words defined previously in the present document or by other referenced
specifications, italics format is used.
EXAMPLE 5: ListRelationship, GeoProperty, Geometry, Second, Number.
When referring to literal strings double quotes are used.
EXAMPLE 6: "application/json", "Subscription".
When referring to the JSON-LD Context the mnemonic text string @context is used as a placeholder.
All the dates and times are given in UTC format.
EXAMPLE 7: 2018-02-09T11:00:00Z.
The measurement units used in the API are those defined by the International System of Units.
EXAMPLE 8: The distance in geo-queries is provided in meters.
When defining application-specific elements or API extensions the same conventions and syntax guidelines should be
followed.


Annex G (informative):

Localization and Internationalization Support

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause D.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Algorithm for transforming an NGSI-LD Relationship into JSON-LD (ALG1.2)
