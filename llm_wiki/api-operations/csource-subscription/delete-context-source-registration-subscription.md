---
type: NGSI-LD Operation
title: "Delete Context Source Registration Subscription"
description: "5.11.6.1 Description This operation allows deleting an existing Context Source Registration Subscription."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.11.6
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.11.6"
---

5.11.6.1 Description

This operation allows deleting an existing Context Source Registration Subscription.

5.11.6.2 Use case diagram

A context source subscriber can delete a Context Source Registration Subscription as shown in Figure 5.11.6.2-1.

Context Source Subscriber

Query Context Source Registration Subscriptions

NGSI-LD Client

NGSI-LD System

Response with List of Subscriptions

1..*

Subscription Array

query context source registration subscriptions


Figure 5.11.6.2-1: Delete Context Source Registration Subscriptions use case

5.11.6.3 Input data - A subscription identifier (URI).

5.11.6.4 Behaviour

- If the subscription Id is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the subscription id provided does not correspond to any existing subscription in the system then an error of type ResourceNotFound shall be raised.
- Otherwise implementations shall delete the Context Source Registration Subscription and no longer perform notifications concerning that Subscription.

5.11.6.5 Output data

None.

# Related

* [HTTP: Resource: csourceSubscriptions/{subscriptionId}](/http-binding/resource-csourcesubscriptions-subscriptionid.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.11.6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Delete Context Source Registration Subscription
