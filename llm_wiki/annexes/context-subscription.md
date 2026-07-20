---
type: Reference
title: "Context Subscription"
description: "Below there is an example of a Context Subscription."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.4
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.4"
---

Below there is an example of a Context Subscription. It makes use of the @context formerly described.

{

"id": "urn:ngsi-ld:Subscription:mySubscription",
"type": "Subscription",
"entities": [
{


"type": "Vehicle"

}

],
"watchedAttributes": ["speed"],
"q": "speed>50",
"geoQ": {
"georel": "near;maxDistance==2000",
"geometry": "Point",
"coordinates": [-1,100]
},
"notification": {
"attributes": ["speed"],
"format": "keyValues",
"endpoint": {
"uri": "http://my.endpoint.org/notify",
"accept": "application/json"

}

},
"@context": [
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

]

} The subject of the Context Subscription is Entities of Type Vehicle which speed is greater than 50, and located close to a certain area defined by a reference spatial point. Every time the speed (watched Attribute) of a concerned vehicle, changes, a new notification (including the new speed value) will be received in the specified endpoint.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Context Subscription
