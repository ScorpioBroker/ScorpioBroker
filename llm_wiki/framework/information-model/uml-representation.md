---
type: NGSI-LD Clause
title: "UML representation"
description: "This clause is informative and is intended to show how the NGSI-LD information model could be described using UML diagrams."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.2.5
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.2.5"
---

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

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.2.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — UML representation
