---
type: NGSI-LD Clause
title: "Additive Registrations"
description: "For additive registrations, the Context Broker is permitted to hold context data about the Entities and Attributes locally itself, and also obtain data from external sources."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.3.6.2
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.3.6.2"
---

For additive registrations, the Context Broker is permitted to hold context data about the Entities and Attributes
locally itself, and also obtain data from external sources. Context provisioning operations are serviced both locally by
the Context Broker itself, and also distributed on to the registered sources.
An inclusive Context Source Registration specifies that the Context Broker considers all registered
Context Sources as equals and will distribute operations to those Context Sources even if relevant context
data is available directly within the Context Broker itself (in which case, all results will be integrated in the final
response). Data from every Context Source registered by an inclusive Context Source Registration is
requested with an equal priority. This is the default mode of operation.
An auxiliary Context Source Registration never overrides data held directly within a Context Broker.
Auxiliary distributed operations are limited to context information consumption operations (see clause 5.7). Context
data from auxiliary context sources is only included if it is supplementary to the context data otherwise available to the
Context Broker. Auxiliary Context Source Registrations are always accepted as there can never be a
conflict.

# Related

* [Clause 5.7](/api-operations/consumption/context-information-consumption.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.3.6.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Additive Registrations
