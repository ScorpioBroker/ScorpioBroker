---
type: Reference
title: "Possible communication models"
description: "This convention can be leveraged by two different communication models: - Subscription/notification, where both the application and the Context Adapter use NGSI-LD Subscriptions to have the command re"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-H.4.1
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "H.4.1"
---

This convention can be leveraged by two different communication models:

- Subscription/notification, where both the application and the Context Adapter use NGSI-LD Subscriptions to have the command requests delivered to the appropriate handler within the Context Adapter and vice-versa. In this case the Context Adapter acts as a Context Source as well as a Context Consumer.
- Forwarding, which uses the NGSI-LD Registry and a Context Adapter able to federate itself with the Context Broker holding the actuator's Entity, as a means to deliver the commands. In this case the Context Adapter acts as a Context Storage as well as a Context Producer.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause H.4.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Possible communication models
