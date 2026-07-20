---
type: NGSI-LD Clause
title: "Default @context assignment"
description: "If the input provided by an API client does not include any @context, then the implementation shall at minimum assign the Core @context to such an input."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.5
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.5"
---

If the input provided by an API client does not include any @context, then the implementation shall at minimum assign the Core @context to such an input. In addition, the Context Broker implementation may allow configuring a default user @context (with default terms), to be used when no user @context is provided. The Core @context shall always take precedence.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Default @context assignment
