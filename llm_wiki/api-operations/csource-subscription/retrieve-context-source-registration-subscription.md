---
type: NGSI-LD Operation
title: "Retrieve Context Source Registration Subscription"
description: "5.11.4.1 Description This operation allows retrieving an existing Context Source Registration Subscription."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.11.4
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.11.4"
---

5.11.4.1 Description

This operation allows retrieving an existing Context Source Registration Subscription.


5.11.4.2 Use case diagram

A Context Source subscriber can retrieve a specific Context Source Registration Subscription as shown in Figure 5.11.4.2-1.

Figure 5.11.4.2-1: Retrieve Context Source Registration Subscription use case

5.11.4.3 Input data

Id (URI) of the subscription to be retrieved (target subscription).

5.11.4.4 Behaviour

- If the subscription Id is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the identifier provided does not correspond to any existing subscription in the system then an error of type ResourceNotFound shall be raised.
- Otherwise implementations shall query the Context Source Registration Subscriptions and obtain the subscription data to be returned to the caller.

5.11.4.5 Output data

A JSON-LD object representing the subscription details as mandated by clause 5.2.12.

# Related

* [HTTP: Resource: csourceSubscriptions/{subscriptionId}](/http-binding/resource-csourcesubscriptions-subscriptionid.md)
* [Clause 5.2.12](/api-operations/data-types/subscription.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.11.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Retrieve Context Source Registration Subscription
