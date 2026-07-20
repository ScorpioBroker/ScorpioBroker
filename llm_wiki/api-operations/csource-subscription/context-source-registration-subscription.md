---
type: NGSI-LD Clause
title: "Context Source Registration Subscription"
description: "5.11.1 Introduction Context Source Registration Subscriptions in general work like context information subscriptions; however, instead of resulting in notifications with context information, the notif"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.11
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.11"
---

5.11.1 Introduction

Context Source Registration Subscriptions in general work like context information subscriptions; however, instead of resulting in notifications with context information, the notifications contain Context Source Registrations describing Context Sources that can potentially provide the requested context information. If no temporal query is present, only Context Source Registrations for Context Sources providing latest information, i.e. without such time intervals, are considered. If a temporal query is present only Context Source Registrations with matching time intervals, i.e. observationInterval or managementInterval, are considered.

5.11.2 Create Context Source Registration Subscription

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

5.11.3 Update Context Source Registration Subscription

5.11.3.1 Description

This operation allows updating an existing Context Source Registration Subscription.

5.11.3.2 Use case diagram

A context source subscriber can update a Context Source Registration Subscription as shown in Figure 5.11.3.2-1.


Figure 5.11.3.2-1: Update Context Source Registration Subscription use case

5.11.3.3 Input data

- Subscription identifier (URI), the target Context Source Registration Subscription.
- A JSON-LD document representing a Subscription Fragment.

5.11.3.4 Behaviour

- If the Subscription Id is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the data types and restrictions expressed by clause 5.2.12 are not met by the Subscription Fragment, then an error of type BadRequestData shall be raised.
- Then, implementations shall modify the target subscription as mandated by clause 5.5.8.
- Finally, send a notification with all currently matching Context Source Registrations.

5.11.3.5 Output data

None.

5.11.4 Retrieve Context Source Registration Subscription

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

5.11.5 Query Context Source Registration Subscriptions

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

5.11.6 Delete Context Source Registration Subscription

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

5.11.7 Notification behaviour

A Context Source Notification is a message that allows a subscriber to be aware of the changes in the set of Context Source Registrations describing Context Sources that can potentially provide the requested context information. Implementations shall exhibit the behaviour described in clause 5.8.6 with the following exceptions:

- If a subscription defines a timeInterval member, a CsourceNotification (clause 5.3.2) shall be sent on initial subscription and periodically, when the time specified time interval (in seconds) has elapsed, regardless of any changes to the set of context source registrations. The CsourceNotification message shall include all the Context Source Registrations whose information property matches the entities and watched Attributes or Attributes specified in the notification parameter and, if present, have a matching geoquery. If either the watched Attributes or the Attributes in the notification are not present or of length 0, all possible Attributes (if present in the Context Source Registrations) for fitting entities match.
- If a subscription does not define a timeInterval term, the csource notification shall be sent on initial subscription and whenever there is a change in a matching csource registration. Such a change may be triggered by the creation of a new matching csource registration, the update of a csource registration (whether matching before the update, after the update or in both cases) or the deletion of a matching csource registration. The notification message shall include the matching csource registration(s) together with the appropriate trigger reason in the triggerReason member.


- Instead of providing the original Context Source Registration which may contain a lot of irrelevant information, implementations should return filtered Context Source Registrations, which only contain context source registration information relevant for the subscription, in particular only matching RegistrationInfo elements.
- A csource notification shall be sent as follows: - The structure of the csource notification message shall be as mandated by clause 5.3.2. - A csource notification shall be sent to the endpoint. - The notification.timesSent member shall be incremented by one. - The notification.lastNotification member shall be updated with the current timestamp. - If the notification is sent successfully: - Update notification.lastSuccess with the current timestamp. - If the notification is not sent successfully: - Update notification.lastFailure with the current timestamp. - Update the subscription status to "failed".

# Related

* [Clause 4.19](/framework/languages/ngsi-ld-scope-query-language.md)
* [Clause 4.9](/framework/languages/ngsi-ld-query-language.md)
* [Clause 5.12](/api-operations/matching/matching-context-source-registrations.md)
* [Clause 5.2.12](/api-operations/data-types/subscription.md)
* [Clause 5.3.2](/api-operations/notifications/csourcenotification.md)
* [Clause 5.5.8](/api-operations/common-behaviours/partial-update-patch-behaviour.md)
* [Clause 5.5.9](/api-operations/common-behaviours/pagination-behaviour.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.11](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Context Source Registration Subscription
