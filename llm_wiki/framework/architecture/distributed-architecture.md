---
type: NGSI-LD Clause
title: "Distributed architecture"
description: "Figure 4.3.3-1 shows a distributed architecture."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.3.3
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.3.3"
---

Figure 4.3.3-1 shows a distributed architecture. The underlying idea here is that all information is stored by the Context Sources. Context Sources implement the query and subscription part of the NGSI-LD API as a Context Broker does. They register themselves with the Context Registry, providing information about what context information they can provide, but not the context information itself, e.g. a certain Context Source registers that it can provide the indoor temperature for Building A and Building B or that it can provide the speed of cars in a geographic region covering the centre of a city.

Figure 4.3.3-1: Distributed architecture


Context Consumers can query or subscribe to the Distribution Broker. On each request, the Distribution Broker discovers or does a discovery subscription to the Context Registry for relevant Context Sources, i.e. those that may provide context information relevant to the respective request from the Context Consumer. The Distribution Broker then queries or subscribes to each relevant Context Source, if possible it aggregates the context information retrieved from the Context Sources and provides them to the Context Consumer. In this mode of operation, it is not visible to the Context Consumer, whether the Context Broker is a Central Broker or a Distribution Broker. Alternatively, the architecture allows that Context Consumers can discover Context Sources through the Context Registry themselves and then directly request from Context Sources. This is shown in Figure 4.3.3-1 with the fine dashed arrows.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.3.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Distributed architecture
