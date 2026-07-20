---
type: NGSI-LD Operation
title: "Delete Subscription"
description: "5.8.5.1 Description This operation allows deleting an existing subscription."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.8.5
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.8.5"
---

5.8.5.1 Description

This operation allows deleting an existing subscription.

5.8.5.2 Use case diagram

A Context Subscriber can delete a subscription within an NGSI-LD system as shown in Figure 5.8.5.2-1.

Context Subscriber

Query Subscriptions

NGSI-LD Client

NGSI-LD System

Response with (Subscription List)

1..*

Subscription Array query subscriptions


Figure 5.8.5.2-1: Delete subscription use case

5.8.5.3 Input data

A subscription identifier (URI).

5.8.5.4 Behaviour

- If the subscription Id is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the subscription id provided does not correspond to any existing subscription in the system, then an error of type ResourceNotFound shall be raised.
- Otherwise, implementations shall delete the Subscription and no longer perform notifications concerning such Subscription.
- Using the mapping of the own Subscription identifier to each of the subscriptionId of a subscription to a Context Source, a delete Subscription shall be forwarded to each such Context Source, if the delete Subscription operation is supported as indicated in the corresponding Context Source Registration: - Based on the mapping of the Subscription to its respective Context Source Registration Subscription (see clause 5.8.1.4), that Context Source Registration Subscription shall be deleted (clause 5.11.6).

5.8.5.5 Output data

None.

# Related

* [HTTP: Resource: subscriptions/{subscriptionId}](/http-binding/resource-subscriptions-subscriptionid.md)
* [Clause 5.11.6](/api-operations/csource-subscription/delete-context-source-registration-subscription.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.8.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Delete Subscription
