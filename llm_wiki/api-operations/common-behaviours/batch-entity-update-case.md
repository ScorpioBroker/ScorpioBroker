---
type: NGSI-LD Clause
title: "Batch Entity Update case"
description: "This operation has two modes of operation, with an optional flag to select between the two."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.11.3
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.11.3"
---

This operation has two modes of operation, with an optional flag to select between the two. The default behaviour is to replace any already existing attributes of the entities, while the optional behaviour is to preserve already existing attributes of the entities. Brokers shall send separate notifications for each individual update, taking throttling into account.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.11.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Batch Entity Update case
