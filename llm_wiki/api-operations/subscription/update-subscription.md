---
type: NGSI-LD Operation
title: "Update Subscription"
description: "5.8.2.1 Description This operation allows updating an existing subscription."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.8.2
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.8.2"
---

5.8.2.1 Description

This operation allows updating an existing subscription.

5.8.2.2 Use case diagram

A Context Subscriber can update an existing subscription within an NGSI-LD system as shown in Figure 5.8.2.2-1.

Figure 5.8.2.2-1: Update subscription use case

5.8.2.3 Input data

- Subscription identifier (URI), the target subscription.
- A JSON-LD document representing a Subscription Fragment.


5.8.2.4 Behaviour

- If the Subscription id is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the NGSI-LD System does not know about the target Subscription, because there is no existing Subscription whose id (URI) is equivalent, an error of type ResourceNotFound shall be raised.
- Execute the behaviour defined in clause 5.5.4 on JSON-LD validation.
- If the data types and restrictions expressed by clause 5.2.12 are not met by the Subscription Fragment, then an error of type BadRequestData shall be raised.
- Term to URI expansion of Attribute names shall be observed as mandated by clause 5.5.7.
- If the jsonldContext field is present and the referenced JSON-LD @context is not available, implementations shall raise an error of type LdContextNotAvailable. If the referenced JSON-LD @context is invalid, implementations shall raise an error of type BadRequestData.
- Then, implementations shall modify the target Subscription as mandated by clause 5.5.8.
- Finally, the following extra behaviour shall be observed when updating Subscriptions: - If isActive is equal to true and expiresAt is not present, then status shall be updated to "active", if and only if, the previous value of status was different than "expired". - If isActive is equal to true and expiresAt corresponds to a DateTime in the future, then status shall be updated to "active". - If isActive is equal to false and expiresAt is not present, then status shall be updated to "paused", if and only if, the previous value of status was different than "expired". - If only expiresAt is included and refers to a DateTime in the future, then status shall be updated to "active", if and only if the previous value of status was "expired". - If expiresAt is included but referring to a DateTime in the past, then a BadRequestData error shall be raised, regardless the value of isActive. - Based on the mapping of the Subscription to its respective Context Source Registration Subscription (see clause 5.8.1.4), that Context Source Registration Subscription shall be updated (clause 5.11.3).

5.8.2.5 Output data

None.

# Related

* [HTTP: Resource: subscriptions/{subscriptionId}](/http-binding/resource-subscriptions-subscriptionid.md)
* [Clause 5.11.3](/api-operations/csource-subscription/update-context-source-registration-subscription.md)
* [Clause 5.2.12](/api-operations/data-types/subscription.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
* [Clause 5.5.8](/api-operations/common-behaviours/partial-update-patch-behaviour.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.8.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Update Subscription
