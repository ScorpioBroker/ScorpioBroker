---
type: Reference
title: "Subscription/notification model"
description: "For the interaction to work, the Context Adapter, acting as a proxy to the actuator, subscribes to all command properties; in example 1 of clause H.3.2, these are \"set-brightness\", \"set-saturation\", \""
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-H.4.2
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "H.4.2"
---

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

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause H.4.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Subscription/notification model
