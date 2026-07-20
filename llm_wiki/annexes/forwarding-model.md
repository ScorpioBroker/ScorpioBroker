---
type: Reference
title: "Forwarding model"
description: "The forwarding model uses registrations and forwarding of requests."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-H.4.3
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "H.4.3"
---

The forwarding model uses registrations and forwarding of requests. Actuation of commands is provisioned via registration(s) to the NGSI-LD Registry done by the Context Adapter that states "I am responsible for command property ". When the Application changes the value of a command property, first the NGSI-LD Context Broker asks to the NGSI-LD Registry whether the property is delegated to some other component. The NGSI-LD Registry knows that property of the Entity is delegated to the Context Adapter. Hence, the request is forwarded to the Context Adapter. Similar to the other communication model, the request will then be translated into the proprietary format and forwarded to the actuator using the actuator-specific protocol.

In this model, the NGSI-LD Entity is distributed over two different components, because some of its properties live in the Context Brokers and other properties live in the Context Adapter, as indicated in Figure H.4.3-1 with a dotted

rectangle.

The rest of the workflow, i.e. delivery of status and result messages to the application, is done similarly to the subscription/notification model. The detailed workflow is depicted in Figure H.4.3-1, and can be interpreted as follows:

1) Application updates turn-on command Property with "value": true

2a) Context Broker ask Registry where to forward the request

2b) Context Broker forwards request to Context Adapter

3) Context Adapter updates turn-on-STATUS command Property with "value": "PENDING"

4) Application gets notification of the new value "PENDING"

5) Context Adapter updates is-on regular Property with "value": true

6) Application gets notification with value: true

7) Context Adapter updates turn-on-RESULT command Property with "value": "OK"

8) Application gets notification with of the new value "OK"

{

"id": "urn:ngsi-ld:pHueActuator:light1", "type": "Lamp", "colorRGB": { "type": "Property","value": "0xABABAB"}, "is-on": {"type": "Property","value": true},

"commands": {“type": "Property", "value": ["set-saturation", "set-hue", "turn-on", …]} … "turn-on": {"type": "Property","value": true} "turn-on-STATUS": {"type": "Property","value": "PENDING"} "turn-on-RESULT": {"type": "Property","value": "OK"}

}

5. Update "is-on" Property

Context Consumer

6. Notification

4. and 8. Notifications

1. Update "turn-on" Property

true true

NGSI-LD system

2. Notification

NGSI-LD Entity

3. and 7. Update "turn-on-STATUS" and "turn-on-RESULT" Properties

Context Adapter


Figure H.4.3-1: Steps of the actuation workflow using forwarding

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause H.4.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Forwarding model
