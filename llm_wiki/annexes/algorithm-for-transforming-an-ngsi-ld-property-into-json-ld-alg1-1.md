---
type: Reference
title: "Algorithm for transforming an NGSI-LD Property into JSON-LD (ALG1.1)"
description: "Let Ps be the Property that has to be transformed."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-D.3
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "D.3"
---

Let Ps be the Property that has to be transformed. It is defined by (P, "AliasP", V, D), where P denotes a Property Type
Id, "AliasP" is the Property name, V is the Property Value and D is the Property Value's data type.
Ps might be associated to extra Properties or Relationships.
Let O be the output JSON-LD object and C the associated JSON-LD context:
1) Execute the following steps:
a) If no member with "AliasP" is present in O, add a new member to O with key "AliasP" and value an
object structure, let it be named Op as defined in the following. Otherwise, add all existing members
with "AliasP" to a JSON-LD array and in addition put the object structure Op as defined in the
following:
- <"type", "Property">.
- If D is not a native JSON data type add a new member to Op with name "value" and which value
has to be an object structure as follows:
1) <"@type", D>.
2) <"@value", V>.
- Else If D is a native JSON data type add a new member to Op as follows:
1) <"value", V>.
b) Add a new member to C as follows:
- <"AliasP", P>.
c) For each Property associated to Ps (Pss) recursively run the present algorithm (ALG1.1) taking the
following inputs:
- Ps → Pss.
- O → Op.
- C → C.


d) For each Relationship associated to Ps (Rss) run algorithm ALG1.2 taking the following inputs: - Rs → Rss. - O → Op. - C → C. 2) Return (O,C) and end of the algorithm.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause D.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Algorithm for transforming an NGSI-LD Property into JSON-LD (ALG1.1)
