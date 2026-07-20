---
type: NGSI-LD Operation
title: "Query Entities"
description: "if supporting asynchronous interactions: 5.11.2 Create CSR Subscription 5.11.3 Update CSR Subscription 5.11.4 Retrieve CSR Subscription 5.11.5 Query CSR Subscription 5.11.6 Delete CSR Subscription Tem"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.7.2
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.7.2"
---

if supporting asynchronous interactions:
5.11.2 Create CSR Subscription
5.11.3 Update CSR Subscription
5.11.4 Retrieve CSR Subscription
5.11.5 Query CSR Subscription
5.11.6 Delete CSR Subscription

Temporal Context Producer

None 5.6.11 Upsert Temporal Evolution of an Entity 5.6.12 Add Attributes to Temporal Evolution of an Entity 5.6.13 Delete Attributes from Temporal Evolution of an Entity 5.6.14 Partial Update Attribute instance 5.6.15 Delete Attribute Instance 5.6.16 Delete Temporal Evolution of an Entity

| Temporal | | 5.7.3 Retrieve Temporal Evolution of an Entity | | | 5.9.2 Register Context Source | |
| --- | --- | --- | --- | --- | --- | --- |
| Context | | 5.7.4 Query Temporal Evolution of Entities | | | 5.9.3 Update CSR | |
| Source | | | | | 5.9.4 Delete CSR | |


Implements Uses

NGSI-LD Role

| Temporal | | | 5.6.11 Upsert Temporal Evolution of an Entity | | | none | |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Context | | | 5.6.12 Add Attributes to Temporal Evolution of an | | | | |
| Repository | | | Entity | | | | |

5.6.11 Upsert Temporal Evolution of an Entity
5.6.12 Add Attributes to Temporal Evolution of an
Entity
5.6.13 Delete Attributes from Temporal Evolution of
an Entity
5.6.14 Partial Update Attribute instance
5.6.15 Delete Attribute Instance
5.6.16 Delete Temporal Evolution of an Entity

| Context | | | 5.9.2 Register Context Source | | | 5.11.7 CSR Notification | |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Registry | | | 5.9.3 Update CSR | | | | |
| | | | 5.9.4 Delete CSR | | | | |
| | | | 5.7.1 Retrieve CSR | | | | |
| | | | 5.7.2 Query CSRs | | | | |
| | | | 5.11.2 Create CSR Subscription | | | | |
| | | | 5.11.3 Update CSR Subscription | | | | |

5.11.4 Retrieve CSR Subscription
5.11.5 Query CSR Subscription
5.11.6 Delete CSR Subscription

# Related

* [HTTP: Resource: entities/](/http-binding/resource-entities.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.7.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Query Entities
