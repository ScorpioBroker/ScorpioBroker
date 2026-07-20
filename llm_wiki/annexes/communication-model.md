---
type: Reference
title: "Communication model"
description: "H.4.1 Possible communication models This convention can be leveraged by two different communication models: - Subscription/notification, where both the application and the Context Adapter use NGSI-LD"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-H.4
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "H.4"
---

H.4.1 Possible communication models

This convention can be leveraged by two different communication models:

- Subscription/notification, where both the application and the Context Adapter use NGSI-LD Subscriptions to have the command requests delivered to the appropriate handler within the Context Adapter and vice-versa. In this case the Context Adapter acts as a Context Source as well as a Context Consumer.
- Forwarding, which uses the NGSI-LD Registry and a Context Adapter able to federate itself with the Context Broker holding the actuator's Entity, as a means to deliver the commands. In this case the Context Adapter acts as a Context Storage as well as a Context Producer.

H.4.2 Subscription/notification model

For the interaction to work, the Context Adapter, acting as a proxy to the actuator, subscribes to all command
properties; in example 1 of clause H.3.2, these are "set-brightness", "set-saturation", "set-hue" and
"turn-on". When the application, acting as the actuation client, updates the value of a command property, the
Context Adapter will receive the notification with the new value. This will be translated into the proprietary format and
forwarded to the actuator using the actuator-specific protocol. The application in turn can subscribe to the command
status and the result. The Context Adapter updates the status of the actuation during the execution of the command,
which is primarily relevant in the case of longer-lasting actuations, and finally updates the result once the actuation has
been completed. If the application has subscribed to the status and result, it will receive the corresponding notifications.
Independent of the command-related properties, the status of the actuator, held within its regular properties, will be
updated.
The detailed workflow is depicted in Figure H.4.2-1, and can be interpreted as follows:
1) Application updates turn-on command Property with "value": true
2) Context Adapter gets notification of the new value true
3) Context Adapter updates turn-on-STATUS command Property with "value": "PENDING"
4) Application gets notification of the new value "PENDING"
5) Context Adapter updates is-on regular Property with "value": true
6) Application gets notification with value: true
7) Context Adapter updates turn-on-RESULT command Property with "value": "OK"
8) Application gets notification with of the new value "OK"


Figure H.4.2-1: Steps of the actuation workflow using subscription/notification

H.4.3 Forwarding model

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

[1] [ETSI GS CIM 009 V1.9.1 clause H.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Communication model
