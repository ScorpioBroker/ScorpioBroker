---
type: Reference
title: "Implementation of the subscription-based actuation workflow"
description: "workflow The Fed4IoT project (https://fed4iot.org) leverages the NGSI-LD architecture and the subscription/notification workflow for actuation, in order to implement the concept of a Cloud of Things."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-H.5
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "H.5"
---

workflow

The Fed4IoT project (https://fed4iot.org) leverages the NGSI-LD architecture and the subscription/notification workflow for actuation, in order to implement the concept of a Cloud of Things. It enables virtualization of existing IoT sensors/actuators through Virtual Things and IoT Brokers. IoT application developers can simply rent the Virtual Things and the Brokers their applications need.

The Fed4IoT's Cloud of Things is named VirIoT (https://github.com/fed4iot/VirIoT), and it is based on the concept of Virtual Silos as-a-service: isolated and secure IoT environments made of Virtual Things whose data can be accessed through standard IoT Brokers (oneM2M, NGSI, NGSI-LD, etc.).

In Figure H.5-1 a diagram shows how VirIoT implements the concept of a large-scale and distribute NGSI-LD system that leverages the architecture and the workflow convention described in clause H.4.2.

{

"id": "urn:ngsi-ld:pHueActuator:light1", "type": "Lamp", "colorRGB": { "type": "Property","value": "0xABABAB"}, "is-on": {"type": "Property","value": true},

"turn-on-STATUS": {"type": "Property","value": "PENDING"} "turn-on-RESULT": {"type": "Property","value": "OK"} }

5. Update "is-on" Property

Context Consumer

6. Notification

4. and 8. Notifications

1. Update "turn-on" Property

true true

NGSI-LD system

Context Broker

3. and 7. Update "turn-on-STATUS" and "turn-on-RESULT" Properties

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause H.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Implementation of the subscription-based actuation workflow
