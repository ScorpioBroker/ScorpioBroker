---
type: NGSI-LD Clause
title: "Centralized architecture"
description: "Figure 4.3.2-1 shows a centralized architecture."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.3.2
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.3.2"
---

Figure 4.3.2-1 shows a centralized architecture. In the centre is a Central Broker that stores all the context information. There are Context Producers that use update operations to update the context information in the Central Broker and there are Context Consumers that request context information from the Central Broker, either using synchronous one-time query or asynchronous subscribe/notify operations. The Central Broker answers all requests from its storage. Figure 4.3.2-1 shows one component that acts as both Context Producer and Context Consumer. The general assumption is that components can have multiple roles, so such components are not explicitly shown in clauses 4.3.3 and 4.3.4.

Figure 4.3.2-1: Centralized architecture

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.3.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Centralized architecture
