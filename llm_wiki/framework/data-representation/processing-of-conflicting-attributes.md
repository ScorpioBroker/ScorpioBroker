---
type: NGSI-LD Clause
title: "Processing of Conflicting Attributes"
description: "In case of conflicting information for an Attribute, where a datasetId is duplicated, but there are differences in the other attribute data, if an expiresAt DateTime is present on the Attribute and th"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.5.3
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.5.3"
---

In case of conflicting information for an Attribute, where a datasetId is duplicated, but there are differences in the other attribute data, if an expiresAt DateTime is present on the Attribute and the date lies in the past, it shall be discarded, thereafter the one with the most recent observedAt DateTime, if present, and otherwise the one with the most recent modifiedAt DateTime shall be provided. If no other mechanism for determining the most current Attribute instance is found, the NGSI-LD system shall choose the Attribute instance at random and the result is indeterminate. When conflicting information is received for the non-reified expiresAt attribute at an Entity level:

- If an expiresAt attribute is missing from at least one version of the Entity received from registered Context Sources, the expiresAt attribute is removed from the Entity.


- Otherwise, the expiresAt attribute of the Entity is set to the DateTime corresponding to the expiresAt DateTime which is furthest in the future.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.5.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Processing of Conflicting Attributes
