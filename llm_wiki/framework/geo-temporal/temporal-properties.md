---
type: NGSI-LD Clause
title: "Temporal Properties"
description: "NGSI-LD defines the following Properties of type TemporalProperty that shall be supported by implementations: - observedAt is defined as the temporal Property at which a certain Property or Relationsh"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.8
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.8"
---

NGSI-LD defines the following Properties of type TemporalProperty that shall be supported by implementations:

- observedAt is defined as the temporal Property at which a certain Property or Relationship became valid or was observed. For example, a temperature Value was measured by the sensor at this point in time.
- createdAt is defined as the temporal Property at which the Entity, Property or Relationship was entered into an NGSI-LD system.
- modifiedAt is defined as the temporal Property at which the Entity, Property or Relationship was last modified in an NGSI-LD system, e.g. in order to correct a previously entered incorrect value.
- deletedAt is defined as the temporal Property at which the Entity, Property or Relationship was deleted from an NGSI-LD system.
- expiresAt is defined as the temporal Property at which the Entity, Property, Relationship, CSourceRegistration, Subscription or Snapshot should be deleted from an NGSI-LD system.


- lastUsedAt is defined as the temporal Property at which a Snapshot has been most recently used, i.e. when the most recent operation has been executed on this Snapshot. Temporal Properties in NGSI-LD shall be represented based on the DateTime data type as mandated by clause 4.6.3.

NOTE 1: For simplicity reasons, a TemporalProperty is represented only by its Value, i.e. no Properties of TemporalProperty nor Relationships of TemporalProperty can be conveyed. In more formal language, a TemporalProperty does not allow reification. NOTE 2: It is important to remark that the term TemporalProperty has been reserved for the semantic tagging of non-reified structural timestamps (observedAt, createdAt, modifiedAt, deletedAt, expiresAt), which capture the temporal evolution of Attributes. Only such structural timestamps can be used as timeproperty in Temporal Queries as mandated by clause 4.11. NOTE 3: User-defined Properties whose value is a time value (Date, DateTime or Time) are defined as Property, not as TemporalProperty, and are serialized in NGSI-LD as shown in annex C, clause C.6. Whenever a TemporalProperty value is unknown by a registered Context Source, the Property shall be omitted rather than sending an error response.

# Related

* [Clause 4.11](/framework/languages/ngsi-ld-temporal-query-language.md)
* [Clause 4.6.3](/framework/restrictions/supported-data-types-for-values.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.8](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Temporal Properties
