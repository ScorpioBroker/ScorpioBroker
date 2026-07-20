---
type: NGSI-LD Data Type
title: "BatchEntityError"
description: "This datatype represents an error raised (associated to a particular Entity) during the execution of a batch or distributed operation."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.17
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.17"
---

This datatype represents an error raised (associated to a particular Entity) during the execution of a batch or distributed
operation.
The supported JSON members shall follow the indications provided in Table 5.2.17-1.
Table 5.2.17-1: BatchEntityError data type definition
Name Data Type Restrictions Cardinality Description
entityId String Valid URI 1 Entity ID corresponding to the Entity in error.
error ProblemDetails
(see IETF
RFC 7807 [10])

1 One instance per Entity in error.

registrationId String Valid URI 0..1 Registration Id corresponding to a failed distributed operation (optional).

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.17](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — BatchEntityError
