---
type: NGSI-LD Clause
title: "Introduction"
description: "One fundamental concept underpinning all of the prototypical architectures described above (clauses 4.3.2, 4.3.3 and 4.3.4) is the idea that Entity data does not need to be centralized within a single"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.3.6.1
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.3.6.1"
---

One fundamental concept underpinning all of the prototypical architectures described above (clauses 4.3.2, 4.3.3 and
4.3.4) is the idea that Entity data does not need to be centralized within a single Context Broker. When reading
context information, a Context Broker can be used as a single point of access to retrieve Entity data found
distributed across multiple associated Context Brokers each receiving a context consumption request. Similarly,
when modifying an Entity, a single request to a Context Broker may result in the operation being distributed and
different parts of that Entity being updated across multiple Context Brokers each receiving a context provision
request.
As long as there is only a centralized Context Broker, i.e. there are no Context Sources registered, all NGSI
LD requests, with few exceptions such as Update Attributes (see clause 5.6.2) and the batch operations (see clauses
5.6.7, 5.6.8, 5.6.9, 5.6.10 and 5.6.20), can either be successfully executed completely, or result in an error. In the
distributed case, all requests can be partially successful. For the centralized case described above, only specific
operations, such as Update Attributes and the batch operations, can be partially successful.
It is the responsibility of the Context Broker to respect the registration parameters when issuing distributed
requests. For instance, if a registration states that only Entities of a given type are offered, the distributed request does
not contain additional types. Such a strict requirement is justified because Context Sources (the receivers of the
distributed request) are not in a position to determine whether a request has been triggered by a registration, rendering
them unable to ensure that the registration parameters are respected in the first place. This applies for any kind of
context data a Context Broker can exchange such as Entity IDs, entity types, attribute names, geofenced areas, etc.
Ultimately, all constraints specified in the registration shall be respected.
When a Context Source is registered, an operation mode is selected. This defines the basis for distributed
operations and also defines whether or not the Context Broker is permitted to hold context data about the Entities
and Attributes locally itself.
If two registered Context Sources are providing context data for the same Attribute, the Attribute instances can be
distinguished by datasetId. The mechanism for determining which data shall be returned is defined in clause 4.5.5.
It is possible to restrict a registered Context Source to operate on a specific Entity type or list of Entity types. In
order for Context Broker hierarchies to support and restrict the distribution of such limited operations, the Entity
type selector (see clause 4.17) can be added as a filter on forwarded requests even where its presence initially seems
redundant.


Furthermore, registered Context Sources may indicate that they are only willing to respond to a limited subset of API operations. Context Brokers shall respect this, to avoid unnecessarily sending distributed operation requests which are always guaranteed to fail. For example, a Context Source may consistently refuse certain API operations since it does not support them. Alternatively, some Context Source endpoints (such as updates) may be protected for use by authorized users only, and not accessible to a Context Broker without those rights. Limited access is likely to be the case in extended data sharing scenarios, where a registered Context Source, and the data held within it, may belong to an external third party. For the endpoints served, all registered Context Sources shall support the normalized representation of Entities as default. Support of additional representation formats is optional and will depend on the implementation. System generated attributes such as modifiedAt and createdAt (see clause 4.8) should be supported by registered Context Sources, at a minimum no error shall be returned if they are not available when requested.

# Related

* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.5.5](/framework/data-representation/multi-attribute-support.md)
* [Clause 4.8](/framework/geo-temporal/temporal-properties.md)
* [Clause 5.6.2](/api-operations/provision/update-attributes.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.3.6.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Introduction
