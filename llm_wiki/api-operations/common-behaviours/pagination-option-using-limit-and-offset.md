---
type: NGSI-LD Clause
title: "Pagination option using limit and offset"
description: "The general pagination behaviour described in clause 5.5.9.1 only requires pointers to the following and previous pages, which can be implemented in a completely opaque way."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.9.2
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.9.2"
---

The general pagination behaviour described in clause 5.5.9.1 only requires pointers to the following and previous pages, which can be implemented in a completely opaque way. Pagination may also be implemented in a transparent way, giving the Context Consumer more control over the process. In this case, the parameters limit and offset should be used, allowing the Context Consumer to adapt the size of the page using limit and jump to a desired set of elements in the results using offset.

# Related

* [Clause 5.5.9.1](/api-operations/common-behaviours/general-pagination-behaviour.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.9.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Pagination option using limit and offset
