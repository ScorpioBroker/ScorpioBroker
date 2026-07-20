---
type: NGSI-LD Clause
title: "NGSI-LD API Structure and Implementation Options"
description: "As stated in clause 4.3.1, the NGSI-LD API is structured into a Core API and an optional Temporal API."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.3.5
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.3.5"
---

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

Context Broker supporting distributed and federated deployments with integrated temporal component and separate Context Registry

local separate

Context Broker supporting distributed and federated deployments with separate temporal component and separate Context Registry

separate separate


Table 4.3.5-3 shows which operations are implemented and used by the other architectural roles as introduced in clause 4.3.2, clause 4.3.3 and clause 4.3.4. In addition, there are separate roles for the Temporal API, i.e. Temporal Context Producer, Temporal Context Source and Temporal Context Consumer. For completeness, the roles of Context Repository and Temporal Context Repository have been introduced, implementing the Context Information Provision and Temporal Context Information Provision functionalities, respectively. In practice, components implementing the latter roles will also implement functionalities for consuming or processing the stored information. Actual components can have multiple roles at the same time, e.g. a Context Broker can implement all roles at the same time. Context Consumers typically only interact with Context Brokers, but in alternative setups, as shown in Figure 4.3.3-1, they can also directly interact with the Context Registry and then directly contact Context Sources.

Table 4.3.5-3: Operations implemented by the various NGSI-LD Roles

NGSI-LD Role

Implements Uses

Context Consumer

5.8.6 Notification - if supporting asynchronous interactions

In case of direct interactions with Context Registry: 5.11.7 CSR Notification - if supporting asynchronous interactions

5.7.1 Retrieve Entity
5.7.2 Query Entities
5.7.5 Retrieve Available Entity Types
5.7.6 Retrieve Details of Available Entity Types
5.7.7 Retrieve Available Entity Type Information
5.7.8 Retrieve Available Attributes
5.7.9 Retrieve Details of Available Attributes
5.7.10 Retrieve Available Attribute Information
5.8.1 Create Subscription
5.8.2 Update Subscription
5.8.3 Retrieve Subscription
5.8.4 Query Subscription
5.8.5 Delete Subscription
5.14.1 Retrieve EntityMap
5.14.2 Update EntityMap
5.14.3 Delete EntityMap
5.14.4 Create EntityMap for Query Entities
5.14.5 Create EntityMap for Query Temporal
Evolution of Entities
5.15.1 Retrieve Context Source Identity Information
5.16.1 Create Snapshot
5.16.2 Clone Snapshot
5.16.3 Retrieve Snapshot Status
5.16.4 Update Snapshot Status
5.16.5 Delete Snapshot
5.16.6 Snapshot Status Notification
In case of direct interactions with Context
Registry:
5.7.1 Retrieve CSR
5.7.2 Query CSRs
if supporting asynchronous interactions:
5.11.2 Create CSR Subscription
5.11.3 Update CSR Subscription
5.11.4 Retrieve CSR Subscription
5.11.5 Query CSR Subscription
5.11.6 Delete CSR Subscription

Context Producer

none 5.6.1 Create Entity 5.6.2 Update Attributes 5.6.3 Append Attributes 5.6.4 Partial Attribute Update 5.6.5 Delete Attribute 5.6.6 Delete Entity 5.6.7 Batch Entity Creation 5.6.8 Batch Entity Upsert 5.6.9 Batch Entity Update 5.6.10 Batch Entity Delete 5.6.17 Merge Entity 5.6.18 Replace Entity 5.6.19 Replace Attribute 5.6.20 Batch Entity Merge

Implements Uses

NGSI-LD Role

Context Source

5.8.6 Notification
5.9.2 Register Context Source
5.9.3 Update CSR
5.9.4 Delete CSR
5.7.1 Retrieve Entity
5.7.2 Query Entities
5.7.5 Retrieve Available Entity Types
5.7.6 Retrieve Details of Available Entity Types
5.7.7 Retrieve Available Entity Type Information
5.7.8 Retrieve Available Attributes
5.7.9 Retrieve Details of Available Attributes
5.7.10 Retrieve Available Attribute Information
5.8.1 Create Subscription
5.8.2 Update Subscription
5.8.3 Retrieve Subscription
5.8.4 Query Subscription
5.8.5 Delete Subscription
5.14.1 Retrieve EntityMap
5.14.2 Update EntityMap
5.14.3 Delete EntityMap
5.14.4 Create EntityMap for Query Entities
5.14.5 Create EntityMap for Query Temporal
Evolution of Entities5.15.1 Retrieve Context Source
Identity Information

| Context | | 5.6.1 Create Entity | | | none | |
| --- | --- | --- | --- | --- | --- | --- |
| Repository | | 5.6.2 Update Attributes | | | | |
| | | 5.6.3 Append Attributes | | | | |

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
5.16.1 Create Snapshot
5.16.2 Clone Snapshot
5.16.3 Retrieve Snapshot Status
5.16.4 Update Snapshot Status
5.16.5 Delete Snapshot
5.16.6 Snapshot Status Notification

Temporal Context Consumer

In case of direct interactions with Context
Registry:
5.11.7 CSR Notification - if supporting
asynchronous interactions
5.7.3 Retrieve Temporal Evolution of an Entity
5.7.4 Query Temporal Evolution of Entities
In case of direct interactions with Context
Registry:
5.7.1 Retrieve CSR

# Related

* [Clause 4.3.1](/framework/architecture/introduction.md)
* [Clause 4.3.2](/framework/architecture/centralized-architecture.md)
* [Clause 4.3.3](/framework/architecture/distributed-architecture.md)
* [Clause 4.3.4](/framework/architecture/federated-architecture.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.3.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD API Structure and Implementation Options
