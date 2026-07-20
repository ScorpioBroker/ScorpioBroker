---
type: NGSI-LD Clause
title: "Transient Storage of Entities and Attributes"
description: "In some cases, it is desirable to create an Entity (or Attribute) which is only expected to be stored for a defined period of time."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.22
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.22"
---

In some cases, it is desirable to create an Entity (or Attribute) which is only expected to be stored for a defined period of
time. Thereafter such an Entity (or Attribute) should be removed, and can be safely deleted from the context via an
automatic garbage collection process.
In this regard NGSI-LD defines the following system Property of type TemporalProperty that shall be supported by
implementations:
- expiresAt is defined as the system temporal Property at which a certain Entity, Property or Relationship shall become invalid and may be automatically removed from the Context Broker. For example, an Alert Entity was created to last for 24 hours and should be removed after this period of time. It should be noted that clean-up processes will only run periodically, and will be dependent upon the Context Broker implementation, therefore final deletion will always lag the expiresAt timestamp to a certain extent. Furthermore, expiresAt only applies to the local storage, i.e. the Entity or Attribute is to be deleted locally, but not on other Context Sources or Context Broker hosting such Entity information, where no expiresAt timestamp is present. Thus expiresAt is not considered to be intrinsic to the Entity or Attribute, but only applies to the storage of the Entity or Attribute respectively. As it pertains to a system function (the deletion from storage after the expiration time), it is considered to be a system attribute.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.22](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Transient Storage of Entities and Attributes
