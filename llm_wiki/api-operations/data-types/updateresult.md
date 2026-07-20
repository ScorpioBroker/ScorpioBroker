---
type: NGSI-LD Data Type
title: "UpdateResult"
description: "This datatype represents the result of Attribute update (append or update) operations in the NGSI-LD API regardless of whether local or distributed."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.18
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.18"
---

This datatype represents the result of Attribute update (append or update) operations in the NGSI-LD API regardless of
whether local or distributed.
The supported JSON members shall follow the indications provided in Table 5.2.18-1.
Table 5.2.18-1: UpdateResult data type definition
Name Data Type Restrictions Cardinality Description
notUpdated NotUpdatedDetails[] See clause 5.2.19 1 List which contains the Attributes (represented
by their name) that were not updated, together
with the reason for not being updated.
updated String[] 1 List of Attributes (represented by their name)
that were appended or updated.

# Related

* [Clause 5.2.19](/api-operations/data-types/notupdateddetails.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.18](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — UpdateResult
