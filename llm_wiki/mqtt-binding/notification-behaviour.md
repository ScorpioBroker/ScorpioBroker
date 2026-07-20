---
type: NGSI-LD Clause
title: "Notification behaviour"
description: "In case a subscription received via HTTP specifies an MQTT endpoint in the \"notification.endpoint.uri\" member of the subscription structure (defined by clauses 5.2.12, 5.2.14 and 5.2.15), and the MQTT"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-7.2
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "7.2"
---

In case a subscription received via HTTP specifies an MQTT endpoint in the "notification.endpoint.uri" member of the
subscription structure (defined by clauses 5.2.12, 5.2.14 and 5.2.15), and the MQTT notification binding is supported by
the NGSI-LD implementation, notifications related to this subscription shall be sent via the MQTT protocol.
The syntax of an MQTT endpoint URI is
mqtt[s]://[][:]@ [:]/ [/]* and
follows an existing convention for representing an MQTT endpoint as a URI [i.19].
Username and password can be optionally specified as part of the endpoint URI. If the port is not explicitly specified,
the default MQTT port is 1883 for MQTT over TCP and 8883 for mqtts, i.e. Secure MQTT over TLS. MQTT supports
the structuring of topics as a hierarchy with any number of subtopic levels, which can be specified as part of the
endpoint URI.
In MQTT, all non-protocol information has to be included into the MQTT message. This means that the actual
notification as specified in clause 5.3.1, as well as additional information like MIME type, possibly the link to the
@context and additional user-specified information, which in the HTTP case is provided as headers, has to be included
into the MQTT message. The MQTT notification message shall be provided as a JSON Object with the two elements
"metadata" and "body". The actual notification, as specified in clause 5.3.1 is the value of "body", whereas any
additional information is provided as key-value pairs in "metadata".
For the MQTT protocol, there are currently two versions supported, MQTTv3.1.1 [24] and MQTTv5.0 [25]. Also, there
are three levels of quality of service:


- at most once (0);

- at least once (1); and

- exactly once (2). These can be specified in the subscription as part of the optional array of KeyValuePair type (defined by clause 5.2.22) "notification.endpoint.notifierInfo". The MQTT protocol parameters can be found in Table 7.2-1. If not present, the given default value is used.

Table 7.2-1: Protocol parameters for MQTT in notifierInfo Key Possible Values Default Source Description MQTT-QoS 0, 1, 2 0 Subscription's notification.endpoint .notifierInfo

MQTT Quality of service, at most once (0), at least once (1) and exactly once (2)

MQTT Version

mqtt3.1.1, mqtt5.0 mqtt5.0 Subscription's notification.endpoint .notifierInfo

Version of MQTT protocol

The MIME type associated with the notification shall be "application/json" by default. However, this can be changed to "application/ld+json" by means of the "endpoint.accept" member. The MIME type is specified as Content-Type in the "metadata" element of the MQTT message. If the target MIME type is "application/json" then the reference to the JSON-LD @context is provided as Link in the "metadata" element of the MQTT message, following the specification of the HTTP Link header as mandated by the JSON-LD specification [2], section 6.2 (to the default JSON-LD @context if none available). Table 7.2-2 lists these "receiver side" metadata parameters.

Table 7.2-2: Parameters for MQTT in "metadata" Key Possible Values Default Source Description Content Type

"application/json
",
"application/ld+j
son"

"applicat ion/json"

Subscription's notification .endpoint.accept

MIME type of the notification included in the "body" element of the MQTT message

Link Same format as specified in JSON-LD specification [2], section 6.2 for the HTTP Link header

Link Header provided in Subscription

Contains the reference to the
@context in case Content-Type is
"application/json".
Example:
;
rel="http://www.w3.org/ns/json
ld#context";
type="application/ld+json"

Additionally, if the optional array of KeyValuePair type (defined by clause 5.2.22) notification.endpoint.receiverInfo of the subscription is present, then a new entry for each member named "key" of the key, value pairs that make up the array shall be generated and added to the "metadata" element of the MQTT message. The content of each entry shall be set equal to the content of the corresponding "value" member of the KeyValuePair.


Annex A (normative): NGSI-LD identifier considerations

# Related

* [Clause 5.2.22](/api-operations/data-types/keyvaluepair.md)
* [Clause 5.3.1](/api-operations/notifications/notification.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 7.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Notification behaviour
