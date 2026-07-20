---
type: NGSI-LD Operation
title: "Query Context Source Registration Subscriptions"
description: "5.11.5.1 Description This operation allows querying existing Context Source Registration Subscriptions."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.11.5
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.11.5"
---

5.11.5.1 Description

This operation allows querying existing Context Source Registration Subscriptions.

5.11.5.2 Use case diagram

A context source subscriber can query all existing Context Source Registration Subscriptions as shown in Figure 5.11.5.2-1.


Figure 5.11.5.2-1: Query Context Source Registration Subscriptions use case

5.11.5.3 Input data

A limit to the number of Context Source Registration Subscriptions to be retrieved. See clause 5.5.9.

5.11.5.4 Behaviour

- The NGSI-LD System shall list all the existing Context Source Registration Subscriptions.
- Pagination logic shall be in place as mandated by clause 5.5.9.

5.11.5.5 Output data

A list (represented as a JSON array) of JSON-LD objects each one representing subscription details as mandated by clause 5.2.12.

# Related

* [HTTP: Resource: csourceSubscriptions/](/http-binding/resource-csourcesubscriptions.md)
* [Clause 5.2.12](/api-operations/data-types/subscription.md)
* [Clause 5.5.9](/api-operations/common-behaviours/pagination-behaviour.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.11.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Query Context Source Registration Subscriptions
