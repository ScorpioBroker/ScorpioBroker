---
type: NGSI-LD Data Type
title: "BatchOperationResult"
description: "This datatype represents the result of a batch operation."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.16
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.16"
---

This datatype represents the result of a batch operation.
The supported JSON members shall follow the indications provided in Table 5.2.16-1.
Table 5.2.16-1: BatchOperationResult data type definition
Name Data Type Restrictions Cardinality Description
errors BatchEntityError[] 1 One array item per Element in error. Empty
Array if no errors happened.
success String[] Array of valid URIs 1 Array of Entity IDs corresponding to the
Elements that were successfully treated by the
concerned operation. Empty Array if no
Element was successfully treated.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.16](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — BatchOperationResult
