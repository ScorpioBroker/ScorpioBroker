---
type: NGSI-LD Clause
title: "NGSI-LD Information Model"
description: "4.2.1 Introduction The NGSI-LD Information Model prescribes the structure of context information that shall be supported by an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.2
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.2"
---

4.2.1 Introduction

The NGSI-LD Information Model prescribes the structure of context information that shall be supported by an
NGSI-LD system. It specifies the data representation mechanisms that shall be used by the NGSI-LD API itself. In
addition, it specifies the structure of the Context Information Management vocabularies to be used in conjunction with
the API.
The NGSI-LD Information Model is defined at two levels (see Figure 4.2.1-1): the foundation classes which correspond
to the Core Meta-model and the Cross-Domain Ontology. The former amounts to a formal specification of the "property
graph" model [i.6]. The latter is a set of generic, transversal classes which are aimed at avoiding conflicting or
redundant definitions of the same classes in each of the domain-specific ontologies. Below these two levels, domain
specific ontologies or vocabularies can be devised. For instance, the SAREF Ontology ETSI TS 103 264 [i.4] can be
mapped to the NGSI-LD Information Model, so that smart home applications will benefit from this Context Information
Management API specification.
The version of the cross-domain model proposed by the present document is a minimal one, aimed at defining the
classes used in this release of the API specification. It has been extended by other work items like ETSI
GS CIM 006 [i.8], with classes defining extra concepts such as mobile vs. stationary entities, instantaneous vs. static
properties, etc.

Figure 4.2.1-1: Overview of the NGSI-LD Information Model Structure

Core MetaModel

Cross-Domain Ontology

Domain-Specific Ontologies


4.2.2 NGSI-LD Meta Model

Figure 4.2.2-1 provides a graphical representation of the NGSI-LD Meta-Model in terms of classes and their relationships. To provide additional clarity an informal (non-normative) mapping to the Property Graph Model is also

presented.

Legend:

[With capital initial]. Used to refer to a class that is a subclass of Entity or Value.

[With capital initial]. Used to refer to a class that is a subclass of Property or Relationship, but which is not itself a property or a relationship. These classes serve as super-classes for a set of properties or relationships in the same domain or aspect.

and

[With small initial]. Used to refer to a proper (direct) class of properties or relationships.

[With small initial and underlined text]. Used to refer to the name of a property that is considered to be "lite" in its informational representation since it shall not be reified, rather a

value is directly attached to it.

[With small or capital initial]. Used to refer to a class or a vocabulary that is inherited from another publicly available standard or ontology.

Figure 4.2.2-1: NGSI-LD Core Meta-Model

Implementations shall support the NGSI-LD Meta-model as follows:

- An NGSI-LD Entity is a subclass of rdfs:Resource [1].

- An NGSI-LD Relationship is a subclass of rdfs:Resource [1].

- An NGSI-LD Property is a subclass of rdfs:Resource [1].

- An NGSI-LD Value shall be either a rdfs:Literal or a node object (in JSON-LD syntax) to represent complex

data structures [1].

- An NGSI-LD Property shall have a value, stated through hasValue, which is of type rdf:Property [1]. An

NGSI-LD Relationship shall have an object stated through hasObject which is of type rdf:Property [1].

Entity Relationship Property hasObject hasValue Value

Literal (rdfs:Literal)

Resource (rdfs:Resource)

Property (rdf:Property)

rdfs:subClassOf rdfs:subClassOf rdfs:subClassOf a a

a = rdf:type rdfs:subClassOf rdfs:domain rdfs:range

N

G

S

I-L

D

R

D

F /

R

D

F

S

4.2.3 Cross Domain Ontology

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

4.2.4 NGSI-LD domain-specific models and instantiation

This clause is informative and is intended to illustrate the relationship between the NGSI-LD Information Model and

NGSI-LD Domain-specific models.

Figure 4.2.4-1 shows an example of an NGSI-LD domain-specific model. Domain-specific models introduce the specific entity types required for a particular domain. Figure 4.2.4-1 shows the types "Car", "Parking", "Street", "Gate". Entity types can have further subtypes, e.g. "OffStreetParking" as subtype of "Parking".

Figure 4.2.4-1: Cross-Domain Ontology and instantiation

In addition, two different NGSI-LD Properties are introduced (hasState, reliability).

The adjacentTo Relationship links entities of type "Parking" with entities of type "Street".

4.2.5 UML representation

This clause is informative and is intended to show how the NGSI-LD information model could be described using UML diagrams. The aim of this diagram is to help those readers less familiar with ontology representations or RDF [1] to

understand the NGSI-LD Information Model.

In Figure 4.2.5-1 NGSI-LD Entity, Relationship, Property and Value are represented as UML classes. UML associations are used to interrelate these classes while keeping the structure and semantics defined by the NGSI-LD

Information Model.

Entity Relationship Property hasObject hasValue Value

Literal (rdfs:Literal)

Resource (rdfs:Resource)

Property (rdf:Property)

rdfs:subClassOf rdfs:subClassOf rdfs:subClassOf a a

a = rdf:type rdfs:subClassOf or rdfs:subPropertyOf rdfs:domain rdfs:range

N

G

S

I-L

D

N

G

S

I-L

D

R

D

F /

R

D

F

S

D

F

Parking Street Gate Car adjacentTo hasOpening hasState reliability

rdfs:subClassOf “Entity”: rdfs:subClassOf “Relationship”: rdfs:subClassOf “Property”:

ParkingA _:adjacentTo#1 StreetA

_:operationSpace#1

GateA adjacentTo operationSpace hasObject _:hasOpening#1 _:hasState#1 hasState

30% busy hasValue _:reliability#1 90% reliability hasValue

hasObject hasOperning

2018-01-01T00:00:00Z 2018-01-01T00:00:00Z

createdAt modifiedAt

2018-01-01T00:00:00Z 2018-01-01T00:00:00Z

createdAt modifiedAt

2018-01-01T00:00:00Z 2018-01-01T00:00:00Z

createdAt modifiedAt

2018-01-01T00:00:00Z 2018-01-01T00:00:00Z

createdAt modifiedAt [{0,0},{0,1},{1,1},{1,0}]

polygon hasValue

type instance

Temporal Property deletedAt createdAt modifiedAt observedAt

Geo Property location observation Space operation Space unitCode

TimeInterval

GeoJSON: Geometry

GeoJSON: Point

GeoJSON: LineString

GeoJSON:Polygon startAt endAt

Language Property

hasLanguageMap LanguageMap Vocab Property

hasVocab

List Relationship

List Property hasValue Lists

JSON Property hasJSON hasObject Lists


Figure 4.2.5-1: NGSI-LD information model as UML

# Related

* [Clause 4.18](/framework/languages/ngsi-ld-scopes.md)
* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.2](/framework/data-representation/ngsi-ld-property-representations.md)
* [Clause 4.6.3](/framework/restrictions/supported-data-types-for-values.md)
* [Clause 4.7](/framework/geo-temporal/geospatial-properties.md)
* [Clause 4.8](/framework/geo-temporal/temporal-properties.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Information Model
