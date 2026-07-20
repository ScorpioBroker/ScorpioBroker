---
type: NGSI-LD Clause
title: "Context Information Management Framework"
description: "4.1 Introduction The present clause describes the technical design principles behind the context information management framework supported by NGSI-LD."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4"
---

4.1 Introduction

The present clause describes the technical design principles behind the context information management framework supported by NGSI-LD. As stated in clause 3.1, the letters "NGSI-LD" which are part of most terms, to confirm that they are distinct from other terms of similar/same name in use in other organizations, are generally omitted in the present document for brevity. In the present document, a number of rather obvious typographic conventions and syntax guidelines are followed, and the reader is referred to annex F for details.

4.2 NGSI-LD Information Model

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

4.3 NGSI-LD Architectural Considerations

4.3.1 Introduction

The NGSI-LD API is intended to be primarily an API and does not define a specific architecture. It is envisioned that
the NGSI-LD API can be used in different architectural settings and the architectural assumptions of the API are kept to
a minimum.
As it is not possible to elaborate all possible architectures in which the NGSI-LD API could be used, three prototypical
architectures are presented. The NGSI-LD API shall enable efficient support for all of them, i.e. the design decisions for
the NGSI-LD API take these prototypical architectures into consideration. A real system architecture utilizing the
NGSI-LD API can map to one, take elements from multiple or combine all of the prototypical architectures.
The NGSI-LD API implicitly defines two sets of Entities:
- the "current state";
- the "temporal evolution" (both the past and possibly future predictions). The NGSI-LD API is structured into a Core API and an optional Temporal API. The Core API manages the current state of Entities. The Temporal API is optional and manages the Temporal Evolution of Entities. Brokers that intend to implement the Temporal API should consider updating the Temporal Evolution of an Entity whenever the "current state" is modified via the Core API.


4.3.2 Centralized architecture

Figure 4.3.2-1 shows a centralized architecture. In the centre is a Central Broker that stores all the context information. There are Context Producers that use update operations to update the context information in the Central Broker and there are Context Consumers that request context information from the Central Broker, either using synchronous one-time query or asynchronous subscribe/notify operations. The Central Broker answers all requests from its storage. Figure 4.3.2-1 shows one component that acts as both Context Producer and Context Consumer. The general assumption is that components can have multiple roles, so such components are not explicitly shown in clauses 4.3.3 and 4.3.4.

Figure 4.3.2-1: Centralized architecture

4.3.3 Distributed architecture

Figure 4.3.3-1 shows a distributed architecture. The underlying idea here is that all information is stored by the Context Sources. Context Sources implement the query and subscription part of the NGSI-LD API as a Context Broker does. They register themselves with the Context Registry, providing information about what context information they can provide, but not the context information itself, e.g. a certain Context Source registers that it can provide the indoor temperature for Building A and Building B or that it can provide the speed of cars in a geographic region covering the centre of a city.

Figure 4.3.3-1: Distributed architecture


Context Consumers can query or subscribe to the Distribution Broker. On each request, the Distribution Broker discovers or does a discovery subscription to the Context Registry for relevant Context Sources, i.e. those that may provide context information relevant to the respective request from the Context Consumer. The Distribution Broker then queries or subscribes to each relevant Context Source, if possible it aggregates the context information retrieved from the Context Sources and provides them to the Context Consumer. In this mode of operation, it is not visible to the Context Consumer, whether the Context Broker is a Central Broker or a Distribution Broker. Alternatively, the architecture allows that Context Consumers can discover Context Sources through the Context Registry themselves and then directly request from Context Sources. This is shown in Figure 4.3.3-1 with the fine dashed arrows.

4.3.4 Federated architecture

The federated architecture shown in Figure 4.3.4-1 is used in cases where existing domains are to be federated. For example, different departments in a city operate their own Context Broker-based NGSI-LD infrastructure, but applications should be able to easily access all available information using just one point of access. The architecture works in the same way as the distributed architecture described in clause 4.3.3, except that instead of simple Context Sources, whole domains are registered with the respective Context Broker as point of access. Typically, the domains will be registered to the federation Context Registry on a more coarse-grained level, providing scopes, in particular geographic scopes, that can then be matched to the scopes provided in the requests. For example, instead of registering individual entities like buildings, the domain would be registered with having information about entities of type building within a geographic area. Applications then query or subscribe for entities within a geographic scope, e.g. buildings in a certain area of the city. The Federation Broker discovers the domain Context Brokers that can provide relevant information, forwards the request to these Context Brokers and aggregates the results, so the application gets the result in the same way as in the centralized and distributed cases.

Figure 4.3.4-1: Federated architecture

A domain itself can use a centralized or distributed architecture or could even utilize a federated architecture that federates sub-domains. As in the distributed case, it is also possible that applications discover relevant domains through the federation-level Context Registry and directly contact the Context Brokers in the individual domains.


4.3.5 NGSI-LD API Structure and Implementation Options

As stated in clause 4.3.1, the NGSI-LD API is structured into a Core API and an optional Temporal API. To support the distributed and federated architectures described in clauses 4.3.3 and 4.3.4 respectively, distributed versions of the operations are needed. They are listed separately under Distributed API and require the operations of the Registry API. The Registry API consists of the operations to be implemented by the Context Registry. Furthermore, the JSONLDContext API provides functionality for storing, managing, and serving JSON-LD @contexts. The APIs are structured according to their functionalities, which is also reflected in how the operations are structured in clause 5. Table 4.3.5-1 introduces the API structure, the respective functionalities and lists the operations for each functionality, pointing to the clauses in which they are defined. The distributed versions of the operations are separately shown in Table 4.3.5-1 under Distributed API, but there is a single clause for each of the operations describing both the centralized and distributed behaviour. In addition, the Distributed API has support operations only needed in distributed and federated architectures.

Table 4.3.5-1: NGSI-LD API structure API Functionality Operations Core API Context Information Provision - operations for providing or managing Entities and Attributes

5.6.1 Create Entity
5.6.2 Update Attributes
5.6.3 Append Attributes
5.6.4 Partial Attribute Update
5.6.5 Delete Attribute
5.6.6 Delete Entity
5.6.7 Batch Entity Creation
5.6.8 Batch Entity Upsert
5.6.9 Batch Entity Update
5.6.10 Batch Entity Delete
5.6.17 Merge Entity
5.6.18 Replace Entity
5.6.19 Replace Attribute
5.6.20 Batch Entity Merge

Context Information Consumption operations for consuming Entities and checking for which Entity Types and Attributes Entities are available in the system

5.7.1 Retrieve Entity
5.7.2 Query Entities
5.7.5 Retrieve Available Entity Types
5.7.6 Retrieve Details of Available Entity Types
5.7.7 Retrieve Available Entity Type Information
5.7.8 Retrieve Available Attributes
5.7.9 Retrieve Details of Available Attributes
5.7.10 Retrieve Available Attribute Information

Context Information Subscription - operations for subscribing to Entities, receiving notifications and managing subscriptions

5.8.1 Create Subscription
5.8.2 Update Subscription
5.8.3 Retrieve Subscription
5.8.4 Query Subscription
5.8.5 Delete Subscription
5.8.6 Notification

Temporal API Temporal Context Information Provision operations for providing or managing the Temporal Evolution of Entities and Attributes

5.6.11 Upsert Temporal Evolution of an Entity
5.6.12 Add Attributes to Temporal Evolution of an
Entity
5.6.13 Delete Attributes from Temporal Evolution of
an Entity
5.6.14 Partial Update Attribute instance
5.6.15 Delete Attribute Instance
5.6.16 Delete Temporal Evolution of an Entity

Temporal Context Information Consumption operations for consuming the Temporal Evolution of Entities

5.7.3 Retrieve Temporal Evolution of an Entity
5.7.4 Query Temporal Evolution of Entities


API Functionality Operations Distributed API Distributed Context Information Provision operations for providing or managing Entities and Attributes

5.6.1 Create Entity (distributed)
5.6.2 Update Attributes (distributed)
5.6.3 Append Attributes (distributed)
5.6.4 Partial Attribute Update (distributed)
5.6.5 Delete Attribute (distributed)
5.6.6 Delete Entity (distributed)
5.6.7 Batch Entity Creation (distributed)
5.6.8 Batch Entity Upsert (distributed)
5.6.9 Batch Entity Update (distributed)
5.6.10 Batch Entity Delete (distributed)
5.6.17 Merge Entity (distributed)
5.6.18 Replace Entity (distributed)
5.6.19 Replace Attribute (distributed)
5.6.20 Batch Entity Merge (distributed)

Distributed Context Information Consumption - operations for consuming Entities and checking for which Entity Types and Attributes Entities are available in the system

5.7.1 Retrieve Entity (distributed)
5.7.2 Query Entities (distributed)
5.7.5 Retrieve Available Entity Types (distributed)
5.7.6 Retrieve Details of Available Entity Types
(distributed)
5.7.7 Retrieve Available Entity Type Information
(distributed)
5.7.8 Retrieve Available Attributes (distributed)
5.7.9 Retrieve Details of Available Attributes
(distributed)
5.7.10 Retrieve Available Attribute Information
(distributed)

Distributed Context Information Subscription - operations for subscribing to Entities, receiving notifications and managing subscriptions

5.8.1 Create Subscription (distributed)
5.8.2 Update Subscription (distributed)
5.8.3 Retrieve Subscription (distributed)
5.8.4 Query Subscription (distributed)
5.8.5 Delete Subscription (distributed)
5.8.6 Notification (distributed)

Distributed Temporal Context Information Provision - operations for providing or managing the Temporal Evolution of Entities and Attributes

5.6.11 Upsert Temporal Evolution of an Entity
(distributed)
5.6.12 Add Attributes to Temporal Evolution of an
Entity (distributed)
5.6.13 Delete Attributes from Temporal Evolution
of an Entity (distributed)
5.6.14 Partial Update Attribute instance (distributed)
5.6.15 Delete Attribute Instance (distributed)
5.6.16 Delete Temporal Evolution of an Entity
(distributed)

Distributed Temporal Context Information Consumption - operations for consuming the Temporal Evolution of Entities

5.7.3 Retrieve Temporal Evolution of an Entity (distributed) 5.7.4 Query Temporal Evolution of Entities (distributed) Support operations for distributed operations 5.14.1 Retrieve EntityMap 5.14.2 Update EntityMap 5.14.3 Delete EntityMap 5.14.4 Create EntityMap for Query Entities 5.14.5 Create EntityMap for Query Temporal Evolution of Entities 5.15.1 Retrieve Context Source Identity Information

Registry API Context Source Registration - operations for registering Context Sources and managing Context Source Registrations (CSRs)

5.9.2 Register Context Source
5.9.3 Update CSR
5.9.4 Delete CSR

Context Source Discovery - operations for retrieving and discovering CSRs

5.7.1 Retrieve CSR
5.7.2 Query CSRs

Context Source Registration Subscription operations for subscribing to CSRs, receiving notifications and managing CSRs

5.11.2 Create CSR Subscription
5.11.3 Update CSR Subscription
5.11.4 Retrieve CSR Subscription
5.11.5 Query CSR Subscription
5.11.6 Delete CSR Subscription
5.11.7 CSR Notification


API Functionality Operations Snapshot API Operations for creating and managing Snapshots.

5.16.1 Create Snapshot
5.16.2 Clone Snapshot
5.16.3 Retrieve Snapshot Status
5.16.4 Update Snapshot Status
5.16.5 Delete Snapshot
5.16.6 Snapshot Status Notification

JSONLDContext API

Storing, managing and serving @contexts 5.13.2 Add @context 5.13.3 List @contexts 5.13.4 Serve @context 5.13.5 Delete and Reload @context

All Context Brokers shall implement the Core API. Context Brokers supporting distributed and federated deployments shall also implement the Distributed API. Temporal API and Registry API can be implemented by a Broker or by a separate temporal component and Context Registry respectively. Table 4.3.5-2 shows the possible implementation configurations. A temporal component implementing the Temporal API can also be used completely independently of a Context Broker. The Snapshot API and the JSONLDContext API are optional. The managing and serving of @contexts can also be handled by an independent, stand-alone component.

Table 4.3.5-2: Main implementation configurations Description Temporal API Registry API Central Broker without temporal support none none Central Broker with integrated temporal component local none Central Broker with separate temporal component separate none Context Broker supporting distributed and federated deployments without temporal support and with integrated Context Registry

none local

Context Broker supporting distributed and federated deployments with integrated temporal component and integrated Context Registry

local local

Context Broker supporting distributed and federated deployments with separate temporal component and integrated Context Registry

separate local

Context Broker supporting distributed and federated deployments without temporal support and separate Context Registry

none separate

Context Broker supporting distributed and federated deployments with integrated temporal component and separate C

… (truncated; see full clause in the PDF).

# Related

* [Clause 3.1](/overview/terms.md)
* [Clause 4.18](/framework/languages/ngsi-ld-scopes.md)
* [Clause 4.3.1](/framework/architecture/introduction.md)
* [Clause 4.3.3](/framework/architecture/distributed-architecture.md)
* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.2](/framework/data-representation/ngsi-ld-property-representations.md)
* [Clause 4.6.3](/framework/restrictions/supported-data-types-for-values.md)
* [Clause 4.7](/framework/geo-temporal/geospatial-properties.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Context Information Management Framework
