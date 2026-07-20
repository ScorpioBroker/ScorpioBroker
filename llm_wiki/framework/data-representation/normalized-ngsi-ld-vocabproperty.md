---
type: NGSI-LD Clause
title: "Normalized NGSI-LD VocabProperty"
description: "An NGSI-LD VocabProperty shall be represented in normalized representation by a member whose key is the Property name (a term), whose value is the same as the JSON-LD object in NGSI-LD Property Repres"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.20.2
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.20.2"
---

An NGSI-LD VocabProperty shall be represented in normalized representation by a member whose key is the
Property name (a term), whose value is the same as the JSON-LD object in NGSI-LD Property Representation
defined in clause 4.5.2.2, with the following differences:
Mandatory
- "type": the fixed value "VocabProperty".
- "vocab": a JSON object consisting of a single string or array of strings which can be type coerced into an IRI or array of IRIs. It represents a more specialized value. Output Only
- "previousVocab": only provided in case of notifications and only if the showChanges option is explicitly requested. It represents the previous VocabProperty vocab, before the triggering change. The representation is the same as that of "vocab". Furthermore, an NGSI-LD VocabProperty in the normalized representation shall never include the following members: Prohibited
- "unitCode": shall never be present, as vocabs are always strings and hence unitless.
- "value" and "previousValue": shall never be present, as value is a generalization of vocab.

# Related

* [Clause 4.5.2.2](/framework/data-representation/normalized-ngsi-ld-property.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.20.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Normalized NGSI-LD VocabProperty
