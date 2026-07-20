---
type: NGSI-LD Clause
title: "NGSI-LD Meta Model"
description: "Figure 4.2.2-1 provides a graphical representation of the NGSI-LD Meta-Model in terms of classes and their relationships."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.2.2
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.2.2"
---

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

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.2.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Meta Model
