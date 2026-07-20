---
type: NGSI-LD Clause
title: "Cross Domain Ontology"
description: "rdfs:subClassOf or rdfs:subPropertyOf rdfs:subClassOf or rdfs:subPropertyOf g g S S in in F F a = rdf:type a = rdf:type D D d d R rdfs:domain rdfs:range rdfs:domain rdfs:range F F ro ro Literal (rdfs:"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.2.3
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.2.3"
---

rdfs:subClassOf or rdfs:subPropertyOf rdfs:subClassOf or rdfs:subPropertyOf

g g

S

S in in

F

F

a = rdf:type a = rdf:type

D

D d d

R

rdfs:domain rdfs:range rdfs:domain rdfs:range

F

F ro ro

Literal (rdfs:Literal)

Resource (rdfs:Resource)

Literal (rdfs:Literal)

Resource (rdfs:Resource)

Property (rdf:Property)

Property (rdf:Property)

D

D

G

G

R

R l l

rdfs:subClassOf rdfs:subClassOf rdfs:subClassOf a a rdfs:subClassOf rdfs:subClassOf rdfs:subClassOf a a

D

D -M -M

I-L

I-L

Entity Relationship Property hasObject hasValue Value

Entity Relationship Property hasObject hasValue Value

ta ta

S

S e e

G

G

N

M

N

GeoJSON: Geometry

JSON Property

hasLanguageMap LanguageMap Vocab Property

List Relationship

Language Property

List Property

Geo Property

Temporal Property unitCode

D

D -D -D

I-L

I-L lo lo

| lo lo s s to to s s | | | | hasJSON | | | | location | | observedAt | | | | | hasVocab | GeoJSON: | | GeoJSON: | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| S S ro ro | | | | | | | | observation | | | | | | | TimeInterval | Point | | LineString | | | |
| n n G G | | | | hasValue | | | | | | modifiedAt | | | | | | | | | | | |
| N N C C O O | | | | | | | | | Space | | | | | | | | | | | | |
| | | | | | Lists | | | | operation | createdAt | | | | startAt | | GeoJSON:Polygon | | | | | |
| | | | | hasObject | | | | | Space | | | | | | | | | | | | |
| | | | | | Lists | | | | | | deletedAt | | | | endAt | | | | | | |

Legend:

[With capital initial]. Used to refer to a class that is a subclass of Entity or Value.

[With capital initial]. Used to refer to a class that is a subclass of Property or Relationship, but which is not itself a property or a relationship. These classes serve as super-classes for a set of properties or relationships in the same domain or aspect.

[With small initial]. Used to refer to a proper (direct) class of properties or relationships.

and

[With small initial and underlined text]. Used to refer to the name of a property that is considered to be "lite" in its informational representation since it shall not be reified, rather a

value is directly attached to it.

[With small or capital initial]. Used to refer to a class or a vocabulary that is inherited from another publicly available standard or ontology.

Figure 4.2.3-1: NGSI-LD Core Meta-Model plus the Cross-Domain Ontology

Figure 4.2.3-1 describes the concepts introduced by the NGSI-LD Cross-Domain Ontology, which shall be supported by implementations as follows:

- Geo Properties: Are intended to convey geospatial information and implementations shall support them as defined in clause 4.7.

- Temporal Properties: Are non-reified Properties (represented only by their Value) that convey temporal information for capturing the time series evolution of other Properties; implementations shall support them as defined in clause 4.8.

- Language Properties: Are intended to convey different versions of the same textual values, whenever a version for each language (for instance: English, Spanish) is needed.

- "unitCode" Property: Is a Property intended to provide the units of measurement of an NGSI-LD Value.

Implementations shall support it as defined in clause 4.5.2.

- "scope" Property: Is a Property that enables putting Entities into a hierarchical structure. Implementations shall support it as defined in clause 4.18.

- LanguageMaps: Are a special type of NGSI-LD Value intended to convey the different values of Language Properties, stated through an hasLanguageMap, which is of type rdf:Property [1] and is itself a subproperty of

hasValue.


- Geometry Values: Are a special type of NGSI-LD Value intended to convey geometries corresponding to geospatial properties. Implementations shall support them as defined in clause 4.7.

- Time Values: Are a special type of NGSI-LD Value intended to convey time instants or intervals representations. Implementations shall support them as defined in clause 4.6.3.

Clause 4.4 defines the Core JSON-LD @context which includes the URIs which correspond to the concepts introduced

above.

# Related

* [Clause 4.18](/framework/languages/ngsi-ld-scopes.md)
* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.2](/framework/data-representation/ngsi-ld-property-representations.md)
* [Clause 4.6.3](/framework/restrictions/supported-data-types-for-values.md)
* [Clause 4.7](/framework/geo-temporal/geospatial-properties.md)
* [Clause 4.8](/framework/geo-temporal/temporal-properties.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.2.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Cross Domain Ontology
