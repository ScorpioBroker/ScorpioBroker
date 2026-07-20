---
type: NGSI-LD Data Type
title: "RegistrationInfo"
description: "The supported JSON members shall follow the requirements provided in Table 5.2.10-1."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.10
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.10"
---

The supported JSON members shall follow the requirements provided in Table 5.2.10-1.

Table 5.2.10-1: RegistrationInfo data type definition Name Data Type Restrictions Cardinality Description entities EntityInfo[] See data type definition in clause 5.2.8. Empty array (0 length) is not allowed. Restrictions in clause 4.3.6 apply as well

0..1 Describes the entities for which the CSource may be able to provide information.

propertyNames String[] Property names as short hand strings or URIs. Empty array is not allowed. Restrictions in clause 4.3.6 apply as well

0..1 Describes the Properties that the CSource may be able to provide.

relationshipNames String[] Relationship names as short hand strings or URIs. Empty array is not allowed. Restrictions in clause 4.3.6 apply as well

0..1 Describes the Relationships that the CSource may be able to provide.

At least one element of RegistrationInfo shall be present.

# Related

* [Clause 4.3.6](/framework/architecture/distributed-operations.md)
* [Clause 5.2.8](/api-operations/data-types/entityinfo.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.10](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — RegistrationInfo
