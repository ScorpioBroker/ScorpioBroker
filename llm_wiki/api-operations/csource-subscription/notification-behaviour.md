---
type: NGSI-LD Operation
title: "Notification behaviour"
description: "A Context Source Notification is a message that allows a subscriber to be aware of the changes in the set of Context Source Registrations describing Context Sources that can potentially provide the re"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.11.7
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.11.7"
---

A Context Source Notification is a message that allows a subscriber to be aware of the changes in the set of Context Source Registrations describing Context Sources that can potentially provide the requested context information. Implementations shall exhibit the behaviour described in clause 5.8.6 with the following exceptions:

- If a subscription defines a timeInterval member, a CsourceNotification (clause 5.3.2) shall be sent on initial subscription and periodically, when the time specified time interval (in seconds) has elapsed, regardless of any changes to the set of context source registrations. The CsourceNotification message shall include all the Context Source Registrations whose information property matches the entities and watched Attributes or Attributes specified in the notification parameter and, if present, have a matching geoquery. If either the watched Attributes or the Attributes in the notification are not present or of length 0, all possible Attributes (if present in the Context Source Registrations) for fitting entities match.
- If a subscription does not define a timeInterval term, the csource notification shall be sent on initial subscription and whenever there is a change in a matching csource registration. Such a change may be triggered by the creation of a new matching csource registration, the update of a csource registration (whether matching before the update, after the update or in both cases) or the deletion of a matching csource registration. The notification message shall include the matching csource registration(s) together with the appropriate trigger reason in the triggerReason member.


- Instead of providing the original Context Source Registration which may contain a lot of irrelevant information, implementations should return filtered Context Source Registrations, which only contain context source registration information relevant for the subscription, in particular only matching RegistrationInfo elements.
- A csource notification shall be sent as follows: - The structure of the csource notification message shall be as mandated by clause 5.3.2. - A csource notification shall be sent to the endpoint. - The notification.timesSent member shall be incremented by one. - The notification.lastNotification member shall be updated with the current timestamp. - If the notification is sent successfully: - Update notification.lastSuccess with the current timestamp. - If the notification is not sent successfully: - Update notification.lastFailure with the current timestamp. - Update the subscription status to "failed".

# Related

* [Clause 5.3.2](/api-operations/notifications/csourcenotification.md)
* [Clause 5.8.6](/api-operations/subscription/notification-behaviour.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.11.7](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Notification behaviour
