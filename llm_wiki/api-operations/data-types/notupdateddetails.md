---
type: NGSI-LD Data Type
title: "NotUpdatedDetails"
description: "This datatype represents additional information provided by an implementation when an Attribute update did not happen."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.19
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.19"
---

This datatype represents additional information provided by an implementation when an Attribute update did not happen. See also clause 5.2.18. The supported JSON members shall follow the indications provided in Table 5.2.19-1.

Table 5.2.19-1: NotUpdatedDetails data type definition
Name Data Type Restrictions Cardinality Description
attributeName String 1 Attribute name.
reason String 1 Reason for not having changed such Attribute.
registrationId String Valid URI 0..1 Registration Id corresponding to a failed
distributed operation (optional).

# Related

* [Clause 5.2.18](/api-operations/data-types/updateresult.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.19](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NotUpdatedDetails
