---
type: NGSI-LD Clause
title: "Batch Entity Creation case"
description: "The first occurrence of an entity in the input array (the oldest one) is used for the creation of the entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.11.1
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.11.1"
---

The first occurrence of an entity in the input array (the oldest one) is used for the creation of the entity. Any subsequent instance of the same entity is reported as an error (entity already exists) in the response.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.11.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Batch Entity Creation case
