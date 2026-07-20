---
type: Reference
title: "Introduction"
description: "The NGSI-LD system has, in addition to the usual NGSI-LD Properties representing the actuator's status, a set of additional, dedicated NGSI-LD Properties associated with: - the list of available comma"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-H.3.0
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "H.3.0"
---

The NGSI-LD system has, in addition to the usual NGSI-LD Properties representing the actuator's status, a set of additional, dedicated NGSI-LD Properties associated with:

- the list of available commands, i.e. the list of commands supported by the actuator;
- command endpoints, one for each command, that are used to send and receive command related messages and optionally hold state for the ongoing commands. The structure of the commands needs to be specified, but not the internal format of their payloads. By using commands with a custom payload, one can support all kinds of operations, for example:
- "set-on": "true"
- "detect-face": {"face-features": "…."}
- "move": "forward" The data model for command requests, status and responses has to include metadata such as the QoS of the command, its identifier, and the custom payload itself. Both the requests/responses and the list of commands the NGSI-LD system is able to support can be represented with additional NGSI-LD Properties, as follows.

Context Consumer Actuator

Command Request Property

Command Execution

Command Status Property

Command Result Property

NGSI-LD system

Context Broker + NGSI-LD Entity

Context Adapter

NGSI-LD API Actuator-specific protocol

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause H.3.0](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Introduction
