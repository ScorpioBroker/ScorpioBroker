---
type: NGSI-LD Clause
title: "Proxied Registrations"
description: "For proxied registrations, the Context Broker itself is not permitted to hold context data about the registered Entities and Attributes locally (thus all registered context data is obtained from the e"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.3.6.3
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.3.6.3"
---

For proxied registrations, the Context Broker itself is not permitted to hold context data about the registered
Entities and Attributes locally (thus all registered context data is obtained from the external registered sources).
Unregistered Attributes of an Entity are permitted to be held locally; when context provisioning operations are received,
registered Attributes are distributed on to the registered sources and never serviced directly by the Context Broker
itself.
An exclusive Context Source Registration specifies that all of the registered context data is held in a single
location external to the Context Broker. The Context Broker itself holds no data locally about the registered
Attributes and no overlapping proxied Context Source Registrations shall be supported for the same
combination of registered Attributes on the Entity. An exclusive registration shall always relate to specific Attributes
found on a single Entity. Thus, the registration shall define both:
- An entity id (i.e. an id pattern or Entity type defining a group of entities is not supported for exclusive registrations).
- Attributes. Once an exclusive Context Source Registration has been created, no further exclusive or redirect Context Source Registrations can be created for that same combination of Entity ID and Attributes. A redirect Context Source Registration also specifies that the registered context data is held in a location external to the Context Broker. It is possible to register (any combination of):
- A whole Entity by id or id pattern (i.e. without specifying individual Attributes in the registration; in this case, all Attributes are held externally).
- Entities by Entity type only (with or without specifying individual Attributes).


- Attributes only. Potentially multiple distinct redirect registrations can apply at the same time. The Context Broker itself holds no data locally in conflict to the registration. In the case that multiple overlapping redirect registrations are defined, operations are distributed to all registered Context Sources.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.3.6.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Proxied Registrations
