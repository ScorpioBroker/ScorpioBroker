---
type: NGSI-LD Clause
title: "Federated architecture"
description: "The federated architecture shown in Figure 4.3.4-1 is used in cases where existing domains are to be federated."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.3.4
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.3.4"
---

The federated architecture shown in Figure 4.3.4-1 is used in cases where existing domains are to be federated. For example, different departments in a city operate their own Context Broker-based NGSI-LD infrastructure, but applications should be able to easily access all available information using just one point of access. The architecture works in the same way as the distributed architecture described in clause 4.3.3, except that instead of simple Context Sources, whole domains are registered with the respective Context Broker as point of access. Typically, the domains will be registered to the federation Context Registry on a more coarse-grained level, providing scopes, in particular geographic scopes, that can then be matched to the scopes provided in the requests. For example, instead of registering individual entities like buildings, the domain would be registered with having information about entities of type building within a geographic area. Applications then query or subscribe for entities within a geographic scope, e.g. buildings in a certain area of the city. The Federation Broker discovers the domain Context Brokers that can provide relevant information, forwards the request to these Context Brokers and aggregates the results, so the application gets the result in the same way as in the centralized and distributed cases.

Figure 4.3.4-1: Federated architecture

A domain itself can use a centralized or distributed architecture or could even utilize a federated architecture that federates sub-domains. As in the distributed case, it is also possible that applications discover relevant domains through the federation-level Context Registry and directly contact the Context Brokers in the individual domains.

# Related

* [Clause 4.3.3](/framework/architecture/distributed-architecture.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.3.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Federated architecture
