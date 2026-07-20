---
type: NGSI-LD Data Type
title: "TriggerReasonEnumeration"
description: "The enumeration can take one of the following values: - \"newlyMatching\" - describes the case that the notified Context Source Registration(s) newly match(es) the identified subscription."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.3.3
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.3.3"
---

The enumeration can take one of the following values:

- "newlyMatching" - describes the case that the notified Context Source Registration(s) newly match(es) the identified subscription. This value is used in the first notification and whenever a new Context Source Registration matching the Subscription has been registered, or an existing Context Source Registration that did not match before has been updated in such a way that it matches now.

- "updated" - describes the case that the notified Context Source Registration that was part of a previous notification has been updated, but still matches the Subscription.

- "noLongerMatching" - describes the case that the notified Context Source Registration that was part of a previous notification no longer matches the Subscription, i.e. as a result of an update or because it was deleted.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.3.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — TriggerReasonEnumeration
