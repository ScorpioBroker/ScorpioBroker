---
type: NGSI-LD Data Type
title: "Endpoint"
description: "This datatype represents the parameters that are required in order to define an endpoint for notifications."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.15
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.15"
---

This datatype represents the parameters that are required in order to define an endpoint for notifications. This can
include, in addition the endpoint's URI, a generic{key, value} array, named receiverInfo, which contains, in a
generalized form, whatever extra information the Context Broker shall convey to the receiver in order for the
Context Broker to successfully communicate with receiver (e.g. Authorization material), or for the receiver to
correctly interpret the received content (e.g. the Link URL to fetch an @context). Additionally, it can include another
generic{key, value} array, named notifierInfo, which contains the configuration that the Context Broker needs to
know in order to correctly set up the communication channel towards the receiver (e.g. MQTT-Version, MQTT-QoS, in
case of MQTT binding, as defined in clause 7.2).
The supported JSON members shall follow the indications provided in Table 5.2.15-1.

Table 5.2.15-1: Endpoint data type definition Name Data Type Restrictions Cardinality Description uri String Dereferenceable URI 1 URI which conveys the endpoint which will receive the notification.

accept String MIME type. It shall be one of: "application/json" "application/ld+json" "application/geo+json"

0..1 Intended to convey the MIME type
of the notification payload body
(JSON, or JSON-LD, or
GeoJSON). If not present, default
is "application/json".
cooldown Number Greater than 0 0..1 Once a failure has occurred, minimum
period of time in milliseconds which
shall elapse before attempting to
make a subsequent notification to the
same endpoint after failure.
If requests are received before the
cooldown period has expired, no
notification is sent.
notifierInfo KeyValuePair[] 0..1 Generic {key, value} array to set
up the communication channel.
receiverInfo KeyValuePair[] 0..1 Generic {key, value} array to
convey optional information to the
receiver.
timeout Number Greater than 0 0..1 Maximum period of time in
milliseconds which may elapse before
a notification is assumed to have
failed. The NGSI-LD system can
override this value. This only applies
if the binding protocol always returns
a response.

# Related

* [Clause 7.2](/mqtt-binding/notification-behaviour.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.15](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Endpoint
