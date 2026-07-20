---
type: NGSI-LD Clause
title: "Introduction"
description: "An NGSI-LD Property, its value and sub-attributes can be represented in two equally valid lossless formats."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.2.1
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.2.1"
---

An NGSI-LD Property, its value and sub-attributes can be represented in two equally valid lossless formats. The
normalized representation is a JSON-LD document that is complete with respect to mandatory members. The concise
representation is a terser alternative, which makes various implicit assumptions against the payloads and removes
redundancy from them.
Both normalized and concise representation of Properties shall be supported by implementations and can be selected by
Context Consumers through specific request parameters. An example of this representation can be found in annex
C, clause C.2.2.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.2.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Introduction
