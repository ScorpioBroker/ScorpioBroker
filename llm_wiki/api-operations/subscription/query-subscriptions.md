---
type: NGSI-LD Operation
title: "Query Subscriptions"
description: "5.8.4.1 Description This operation allows querying existing Subscriptions."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.8.4
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.8.4"
---

5.8.4.1 Description

This operation allows querying existing Subscriptions.

5.8.4.2 Use case diagram

A Context Consumer can query the existent Subscriptions from an NGSI-LD system as shown in Figure 5.8.4.2-1.


Figure 5.8.4.2-1: Query subscriptions use case

5.8.4.3 Input data

A limit to the number of subscriptions to be retrieved. See clause 5.5.9.

5.8.4.4 Behaviour

- The NGSI-LD system shall list all the existing subscriptions up to the limit specified as input data. If no limit is specified the number of subscriptions retrieved may depend on the implementation.
- Pagination logic shall be in place as mandated by clause 5.5.9.

5.8.4.5 Output data

A list (represented as a JSON array) of JSON-LD objects each one representing subscription details as mandated by clause 5.2.12.

# Related

* [HTTP: Resource: subscriptions/](/http-binding/resource-subscriptions.md)
* [Clause 5.2.12](/api-operations/data-types/subscription.md)
* [Clause 5.5.9](/api-operations/common-behaviours/pagination-behaviour.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.8.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Query Subscriptions
