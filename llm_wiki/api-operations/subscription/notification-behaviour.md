---
type: NGSI-LD Operation
title: "Notification behaviour"
description: "A notification is a message that allows a subscriber to be aware of the changes in subscribed Entities."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.8.6
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.8.6"
---

A notification is a message that allows a subscriber to be aware of the changes in subscribed Entities. Implementations shall exhibit the following behaviour:

- Notifications shall only be sent if and only if the status of the corresponding subscription (subscription.status) is "active", i.e. not "paused" nor "expired".


- If a Subscription defines a timeInterval member, a Notification shall be sent periodically, when the time interval (in seconds) specified in such value field is reached, regardless of Attribute changes. The notification message shall include all the subscribed Entities that match the query, geoquery and Scope query conditions. If none of query, geoquery and Scope query are defined, then all subscribed Entities shall be included. For each entity in the notification, only the Attribute instances are to be included that match the datasetId member in Subscription. If there are no matching Entities, no Notification is sent.
- If a Subscription does not define a timeInterval term, the notification shall be sent whenever there is a change in the watched Attributes. An Attribute is considered to change when any of the members (including children) in its corresponding JSON-LD node is updated with a value different than the existing one. The notification message shall include all the subscribed Entities that changed and that match (as mandated by clauses 4.9, 4.10 and 4.19) the query, geoquery and Scope query conditions. If query, geoquery and scopeQuery are all not defined then all subscribed Entities that changed shall be included. If, for an Entity, there are multiple instances of the GeoProperty on which the geoquery is based, it is sufficient if any of these instances meets the geospatial restrictions. For each entity in the notification, only the Attribute instances are to be included that match the datasetId member in Subscription. Finally, if a Context Source filter is defined, then only the subscribed Entities whose origin Context Source matches the referred filter shall be included.
- If a Notification with a subscriptionId is received that has a mapping to a local Subscription identifier, the Notification shall be copied. If the splitEntities member of the local Subscription is explicitly set to true or, if not explicitly set, the default setting of the deployment allows split entities: - The Entities contained in the data member of the Notification shall be retrieved locally and from all Context Sources that have information about these Entities, except for the one from which the Notification has been received. - The retrieved Entities then shall be merged with the Entities in the data member. - All Entities that do not match the query, geoquery and Scope query conditions of the local Subscription shall be removed from the data member.
- If there are Entities in the data member of the Notification copy, the Notification copy shall be forwarded to the Notification endpoint specified in the local Subscription, using this local Subscription identifier instead of the subscriptionId received.
- A Notification shall be sent as follows: - The structure of the notification message shall be as mandated by clause 5.3.1. - The @context to be used is the one specified in the jsonldContext field of the corresponding Subscription. - The Attributes included (Properties or Relationships) shall be those specified by the notification.attributes member in the Subscription data type (clause 5.2.12). Term to URI expansion shall be observed (clause 5.5.7). The absence of the notification.attributes member of a Subscription means that all Attributes shall be included. If the notification was triggered by the deletion of an Entity and the notification.showChanges member is not set to true, only the deletedAt system property shall be provided. In case notification.sysAttrs is set to true, also createdAt and modifiedAt shall be provided. - If an Attribute has been deleted, only the name of the attribute as key and the URI "urn:ngsi ld:null" as value shall be provided, unless more information is required. The latter is the case, if: - a datasetId needs to be provided; - the notification.sysAttrs is set to true and thus the system generated sub-attributes (see clause 4.8) have to be provided; - notification.showChanges is set to true and thus a previous value or object has to be provided. In all such cases, a JSON object with all the required information is provided, where the value or the object is set to the URI "urn:ngsi-ld:null" respectively or, in case of a LanguageProperty, the languageMap is set to {"@none": "urn:ngsi-ld:null"}.


- If the notification.format member value is set, the representation of the entities changes:
- If the notification.format is set to "concise" then a concise representation of the entities shall
be provided as defined by clause 4.5.1.
- If the notification.format is set to "simplified" (or "keyValues" as a synonym), then a
simplified representation of the entities (as mandated by clause 4.5.4) shall be provided.
- Otherwise the normalized format as defined by clause 4.5.1 shall be used.
- If the notification.pick member value is set, every Entity within the payload body is reduced down to
only contain the defined entity members listed.
- If the notification.omit member value is set, the defined entity members listed are removed from each
Entity within the payload.
- If the notification.join member value is set then Linked Entity retrieval (as mandated by
clause 4.5.23) shall be provided.
- If the ngsildConformance field of the corresponding Subscription is set, the notification shall undergo a
backwards compatibility operation as defined by clause 4.3.6.8 and be amended to conform to the
supplied version of the NGSI-LD specification.
- A Notification shall be sent (as mandated by each concrete binding and including any optional
endpoint.receiverInfo defined by clause 5.2.22) to the endpoint specified by the endpoint.uri member of
the notification structure defined by clause 5.2.14. The Notification content shall be JSON by default.
However, this can be changed to JSON-LD or GeoJSON by means of the endpoint.accept member.
- The notification.timesSent member shall be incremented by one.
- The notification.lastNotification member shall be updated with a timestamp representing the current date
and time.
- If the response to the notification request is 200 OK then implementations shall:
- Update notification.lastSuccess with a timestamp representing the current date and time.
- Update notification.status to "ok".
- If the response to the notification request is different than 200 OK then implementations shall:
- Update notification.lastFailure with a timestamp representing the current date and time.
- Update notification. Status to "failed".

# Related

* [Clause 4.3.6.8](/framework/architecture/backwards-compatibility-of-context-source-payloads.md)
* [Clause 4.5.1](/framework/data-representation/ngsi-ld-entity-representation.md)
* [Clause 4.5.23](/framework/data-representation/ngsi-ld-linked-entity-retrieval.md)
* [Clause 4.5.4](/framework/data-representation/simplified-representation.md)
* [Clause 4.8](/framework/geo-temporal/temporal-properties.md)
* [Clause 5.2.12](/api-operations/data-types/subscription.md)
* [Clause 5.2.14](/api-operations/data-types/notificationparams.md)
* [Clause 5.2.22](/api-operations/data-types/keyvaluepair.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.8.6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Notification behaviour
