---
type: NGSI-LD Data Type
title: "Subscription"
description: "This datatype represents a Context Subscription."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.12
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.12"
---

This datatype represents a Context Subscription.
The supported JSON members shall follow the requirements provided in Table 5.2.12-1.

Table 5.2.12-1: Subscription data type definition Name Data Type Restrictions Cardinality Description id String Valid URI 0..1 Subscription identifier (JSON-LD @id). Generated at creation time, if it is not provided, it will be assigned during subscription process and returned to client. It cannot be later modified in update operations.

1 JSON-LD @type.

type String It shall be equal to "Subscription"

0..1 Entities subscribed.

entities EntitySelector[] See data type definition in clause 5.2.33. Empty array (0 length) is not allowed. Mandatory if timeInterval is present, unless the execution of the request is limited to local scope (see clause 5.5.13)

| notification | | NotificationPara | See data type definition in | | | 1 | Notification details. | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | ms | clause 5.2.14 | | | | | | |
| notificationTrigger | | String[] | Valid notification triggers are | | | 0..1 | The notification triggers listed | | |
| | | | "entityCreated", | | | | indicate what kind of changes shall | | |
| | | | "entityUpdated", | | | | trigger a notification. If not present, | | |
| | | | "entityDeleted", | | | | the default is the combination | | |
| | | | "attributeCreated", | | | | "attributeCreated" and | | |
| | | | "attributeUpdated", | | | | "attributeUpdated". | | |
| | | | "attributeDeleted" | | | | "entityUpdated" is | | |
| | | | | | | | equivalent to the combination | | |
| | | | | | | | "attributeCreated", | | |

"attributeUpdated" and "attributeDeleted".

0..1 Watched Attributes (Properties or Relationships). If not defined it means any Attribute.

watchedAttributes String[] Attribute name as short hand strings or URIs. Empty array (0 length) is not allowed. if timeInterval is present it shall not appear (0 cardinality)

q String A valid query string as per clause 4.9

0..1 Query that shall be met by subscribed entities in order to trigger the notification.

geoQ GeoQuery See data type definition in clause 5.2.13

0..1 Geoquery that shall be met by subscribed entities in order to trigger the notification. scopeQ String See clause 4.19 0..1 Scope query.


Name Data Type Restrictions Cardinality Description temporalQ TemporalQuery See data type definition in clause 5.2.21

0..1 Temporal Query to be used only in Context Registration Subscriptions for matching Context Source Registrations of Context Sources providing temporal information.

csf String A valid query string as per clause 4.9

0..1 Context source filter that shall be met by Context Source Registrations describing Context Sources to be used for Entity Subscriptions.

datasetId String[] Valid URIs, "@none" for including the default Attribute instances.

0..1 Specifies the datasetIds of the
Attribute instances to be selected
for each matched Attribute as per
clause 4.5.5.
description String 0..1 Subscription description.
expandValues String Comma separated list of
attribute names
0..1 Values of the identified attributes
should be expanded against the
supplied @context using JSON-LD
type coercion prior to executing the
query.
expiresAt String DateTime (see clause 4.6.3) 0..1 Expiration date for the
subscription.
isActive Boolean true by default 0..1 Allows clients to temporarily pause
the subscription by making it
inactive. true indicates that the
Subscription is under operation.
false indicates that the subscription
is paused, and notifications shall
not be delivered.

jsonKeys String Comma separate list of attribute names

0..1 Values of the identified attributes are to be considered uninterpretable as JSON-LD and should not be expanded against the supplied @context using JSON-LD type coercion prior to executing the query. jsonldContext String Dereferenceable URI 0..1 The dereferenceable URI of the JSON-LD @context to be used when sending a notification resulting from the subscription. If not provided, the @context used for the subscription shall be used as a default.

lang String A natural language filter in the form of a IETF RFC 5646 [28] language code

0..1 Language filter to be applied to the query (clause 4.15).

localOnly Boolean 0..1 If localOnly=true then the subscription only pertains to the Entities stored locally. In case the Subscription pertains to a Snapshot it is always local, regardless of whether localOnly is set to true or not.

ngsildConformance String A semantically versioned string in the form major.minor, which conforms to a version of the NGSI-LD specification

0..1 If provided the notification shall undergo a backwards compatibility operation as defined by clause 4.3.6.8 and be amended to conform to the supplied version of the NGSI-LD specification.


Name Data Type Restrictions Cardinality Description splitEntities Boolean default decided by implementation; it should be configurable. The parameter does not apply in case localOnly is true.

0..1 If true it is assumed that single Entities are distributed between different Context Brokers and/or Context Sources and this has to be taken into account when applying any kind of filters (q, geoQ, scopeQ, Attributes etc.). If false it is expected that Context Broker and/or Context Source always have complete Entities, which allows applying filters locally. subscriptionName String 0..1 A (short) name given to this Subscription.

throttling Number Greater than 0. Fractional values are allowed. If timeInterval is present it shall not appear (0 cardinality)

0..1 Minimal period of time in seconds which shall elapse between two consecutive notifications.

timeInterval Number Greater than 0 if watchedAttributes is present it shall not appear (0 cardinality)

0..1 Indicates that a notification shall be delivered periodically regardless of attribute changes. Actually, when the time interval (in seconds) specified in this value field is reached.

At least one of (a) entities or (b) watchedAttributes shall be present, unless the member localOnly is set to true, in which case the execution of the request is limited to local scope (see clause 5.5.13). The members (defined by Table 5.2.12-2) of the Subscription data structure are also defined. They are read-only and shall be automatically generated by NGSI-LD implementations. They shall not be provided by Context Subscribers. In the event that they are provided (in update or create operations) NGSI-LD implementations shall ignore them.

Table 5.2.12-2: Additional members of the Subscription data type Name Data Type Restrictions Cardinality Description status String Allowed values: "active" "paused" "expired"

0..1 Read-only. Provided by the system when querying the details of a subscription.

# Related

* [Clause 4.15](/framework/languages/ngsi-ld-language-filter.md)
* [Clause 4.19](/framework/languages/ngsi-ld-scope-query-language.md)
* [Clause 4.3.6.8](/framework/architecture/backwards-compatibility-of-context-source-payloads.md)
* [Clause 4.5.5](/framework/data-representation/multi-attribute-support.md)
* [Clause 4.6.3](/framework/restrictions/supported-data-types-for-values.md)
* [Clause 4.9](/framework/languages/ngsi-ld-query-language.md)
* [Clause 5.2.13](/api-operations/data-types/geoquery.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.12](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Subscription
