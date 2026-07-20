---
type: Reference
title: "Architecture for actuation"
description: "In this architecture, the application acts as Context Consumer, and the terms are used interchangeably."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-H.2
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "H.2"
---

In this architecture, the application acts as Context Consumer, and the terms are used interchangeably.
Commands are sent to the Context Broker by the Context Consumer, using the standard NGSI-LD API and a
suggested convention for representing them. In turn, feedback about command execution is received by the Context
Consumer, both as continuous status updates and/or a final command result.
More specifically, the component that handles direct communication with the actuator is the Context Source or the
Context Producer: it uses an actuator-specific protocol to control the actuator and get responses and updates from
it, i.e. from the real system. Such Context Source/Consumer or Context Producer/Storage acting as a proxy
or intermediary to the actuator is referred to, in the following, as Context Adapter.
Thus, the Context Adapter is able to use the NGSI-LD API to receive NGSI-LD command requests from the NGSI-LD
Context Broker and send back command status and result to it, as well as using an actuator-specific protocol to
communicate with the actuator.
The NGSI-LD Context Broker is responsible for handling direct communication with the Context Consumer.


Thus, to support actuation, there is a need to specify:

- Additional NGSI-LD Properties the NGSI-LD system has to have, in order to represent and manage command Request, Status, Result.
- A communication model that allows commands to flow in forward direction and feedback to flow in reverse. This communication model has to comprise a mapping, to be held within the NGSI-LD system, that is able to route the command requests to the appropriate handler within the Context Adapter and vice-versa.

Figure H.2-1: Architecture for actuation

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause H.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Architecture for actuation
