---
type: NGSI-LD Operation
title: "Create Context Source Registration Subscription"
description: "5.11.2.1 Description This operation allows creating a new Context Source Registration Subscription."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.11.2
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.11.2"
---

5.11.2.1 Description

This operation allows creating a new Context Source Registration Subscription.

5.11.2.2 Use case diagram

A Context Source Subscriber can subscribe to a new Context Source Registration as shown in Figure 5.11.2.2-1.


Figure 5.11.2.2-1: Subscribe Context Source Registration use case

5.11.2.3 Input data

- A data structure (represented in JSON-LD) conforming to the Subscription data type as mandated by clause 5.2.12.

5.11.2.4 Behaviour

- The behaviour shall be as described in clause 5.8.1.4, restricted to the local case (where Subscriptions cannot be received from remote Brokers), with the following exceptions: - If all checks described in clause 5.8.1.4 pass, implementations shall add a new Context Source Registration Subscription. The parameters of the created subscription shall be configured as described in clause 5.2.12. - Instead of directly matching the entities and watched Attributes from the Subscription with the Context Source registrations, the entities specified in the subscription, the watched Attributes and the Attributes specified in the notification parameter are matched against the respective information property of the Context Source registrations. If either the watched Attributes or the Attributes in the notification are not present or of length 0, all possible Attributes (if present in the Context Source Registrations) for matching entities match. This matching is further described in clause 5.12. - If present, the geoquery in the geoQ element is matched against the GeoProperty of the subscription identified in the geoQ element. If no GeoProperty is specified in the geoquery, the default property is 'location'. The geoquery matches the GeoProperty specified in the Context Source Registration, if the location directly matches or if the location possibly contains locations that would match the geoquery. - If no temporal query is present in the temporalQ element, only Context Source Registrations for Context Sources providing latest information, i.e. without specified time intervals for observationInterval or managementInterval, are considered.


- If a temporal query in the temporalQ element is present, only Context Source Registrations
with specified time intervals are considered. If the timeproperty is "observedAt" or no timeproperty
is specified in the temporal query (default: observedAt), the temporal query is matched against the
observationInterval (if present). If the timeproperty is "createdAt", "modifiedAt" or
"deletedAt", the temporal query is matched against the managementInterval (if present). If the
relevant interval is not present, there is no match:
- The semantics of the match is that the timeAt in the case of the "before" and "after" relation
is contained in or is an endpoint of a time period included in the specified time interval. In the case
of the "between" relation there is a match if there is an overlap between the interval specified by
the timeAt and endTimeAt and the specified time interval.
- If present, the conditions specified by the context source filter match the respective Context Source
Properties (as mandated by clause 4.9).
- If the subscription defines a timeInterval term, a CsourceNotification (clause 5.3.2) with all matching Context Source Registrations shall be sent periodically, initially on subscription and when the time interval (in seconds) specified in such value field is reached, independent of any changes to the set of Context Source registrations.
- If timeInterval is not defined, initially on subscription and whenever there is a change of a matching Context Source Registration (creation, update, deletion), implementations shall post a new CsourceNotification to the endpoint specified in the notification parameters informing about this change by providing the Context Source Registration(s) together with the appropriate trigger reason in the triggerReason member.
- If present, the conditions specified by the context source query match the respective Context Source Properties (as mandated by clause 4.9).
- If present, the Scope query (as mandated by clause 4.19) is matched against the scope property.

5.11.2.5 Output data

A URI identifying the newly created Subscription.

# Related

* [HTTP: Resource: csourceSubscriptions/](/http-binding/resource-csourcesubscriptions.md)
* [Clause 4.19](/framework/languages/ngsi-ld-scope-query-language.md)
* [Clause 4.9](/framework/languages/ngsi-ld-query-language.md)
* [Clause 5.12](/api-operations/matching/matching-context-source-registrations.md)
* [Clause 5.2.12](/api-operations/data-types/subscription.md)
* [Clause 5.3.2](/api-operations/notifications/csourcenotification.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.11.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Create Context Source Registration Subscription
