---
type: NGSI-LD Operation
title: "Retrieve Subscription"
description: "5.8.3.1 Description This operation allows retrieving an existing subscription."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.8.3
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.8.3"
---

5.8.3.1 Description

This operation allows retrieving an existing subscription.

5.8.3.2 Use case diagram

A Context Subscriber can retrieve a specific subscription from an NGSI-LD system as shown in Figure 5.8.3.2-1.


Figure 5.8.3.2-1: Retrieve subscription use case

5.8.3.3 Input data

Id (URI) of the subscription to be retrieved (target subscription).

5.8.3.4 Behaviour

- If the subscription Id is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the identifier provided does not correspond to any existing subscription in the system then an error of type ResourceNotFound shall be raised.
- Otherwise, implementations shall query the subscriptions and obtain the subscription data to be returned to the caller.

5.8.3.5 Output data

A JSON-LD object representing the subscription details as mandated by clause 5.2.12.

# Related

* [HTTP: Resource: subscriptions/{subscriptionId}](/http-binding/resource-subscriptions-subscriptionid.md)
* [Clause 5.2.12](/api-operations/data-types/subscription.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.8.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Retrieve Subscription
