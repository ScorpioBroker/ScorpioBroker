---
type: Reference
title: "Actuators and feedback to the consumer"
description: "Actuators are things that can change their state (light on/off) or execute actions (move forward, detect face, etc.)."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-H.1
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "H.1"
---

Actuators are things that can change their state (light on/off) or execute actions (move forward, detect face, etc.).
There is currently no explicitly and precisely specified support for actuation in the NGSI-LD API. Thus, this clause
describes some conventions that represent a proposed best-practice about how NGSI-LD API and data models can be
used for the interaction between applications and actuators represented by NGSI-LD Entities.
The conventions and approach described in this clause are not powerful enough to implement complex actuation jobs
that depend on each other and, for instance, make actuation decisions conditional on the outcome of other actuations,
unless that behaviour is implemented in a custom way within the application logic. The concept of a more evolved
service execution logic, being a first-class citizen of the NGSI-LD API and able to offer more structured building blocks
for actuation, is outside the scope of this annex.
An NGSI-LD system that comprises an actuator and supports actuation workflows is represented as one or more
NGSI-LD Entities, plus a Context Broker, a Context Source or a Context Producer, and a Context
Consumer, which collaborate.
The interaction between actuator and Context Consumer needs to be bidirectional. Thus, actuators are triggered by
the reception of actuation-specific commands (e.g. "set the on state of the lamp to false", to turn the light off) that are
encoded as NGSI-LD data, following a suggested data model. They respond with feedback, similarly encoded as
NGSI-LD data.
Command feedbacks may serve to control the maximum operations rate a controlling application needs to achieve, and
different levels of feedback can be requested, by associating a specific Quality of Service value to the command:
- Some applications need high operation rate but no feedback. For this case a QoS = 0 can be used. The typical example is to control the arms of a robot with a joystick.
- Some applications need to be sure that the actuators actually received the command request or need to get back a payload in response to the command. In this case a QoS = 1 can be used. The typical case is switching on a light with confirmation, or request face-detection with consequent notification of matching events.
- Commands can either require a short or a long execution time. For commands with long execution time, the application may require a continuous status feedback. In this case a QoS = 2 can be used. The typical example is that of a door opening, where feedback continuously reports the current level (10 % open, 50 % open, etc.).

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause H.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Actuators and feedback to the consumer
