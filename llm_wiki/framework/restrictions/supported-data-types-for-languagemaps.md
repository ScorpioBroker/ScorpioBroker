---
type: NGSI-LD Clause
title: "Supported data types for LanguageMaps"
description: "Compliant NGSI-LD implementations shall support the following data types for representing LanguageMaps: - A JSON object consisting of a series of key-value pairs where the keys shall be JSON strings r"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.6.5
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.6.5"
---

Compliant NGSI-LD implementations shall support the following data types for representing LanguageMaps:

- A JSON object consisting of a series of key-value pairs where the keys shall be JSON strings representing IETF RFC 5646 [28] language codes or the JSON-LD "@none" for representing default when no more specific language is found. and the values shall be JSON strings or arrays of JSON strings. Additionally, the languageMap encoding {"@none": "urn:ngsi-ld:null"} shall be used to represent an NGSI-LD Null during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12) and for representing deleted Language Properties in notifications and in temporal evolutions.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.6.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Supported data types for LanguageMaps
