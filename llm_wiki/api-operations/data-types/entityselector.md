---
type: NGSI-LD Data Type
title: "EntitySelector"
description: "This type selects which entity or group of entities are queried or subscribed to by Context Consumers."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.33
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.33"
---

This type selects which entity or group of entities are queried or subscribed to by Context Consumers. Entities can be specified by their id, Entity Types or group of Entity IDs (as a regular expression pattern mandated by IEEE 1003.2™ [11]). The JSON members shall follow the indications provided in Table 5.2.33-1. id takes precedence over idPattern.

Table 5.2.33-1: EntitySelector data type definition Name Data Type Restrictions Cardinality Description type String A valid type selection string as per clause 4.17. To indicate a request for all Entities (with implied local scope), "*" is also allowed as a value.

1 Selector of Entity Type(s); If type is specified as "*", implying local scope, local scope shall not be explicitly set to be false (clause 5.5.13) for the execution of the corresponding operation.

Valid URI(s) 0..1 Entity identifier(s).

id String or String[]

idPattern String Regular expression as per IEEE 1003.2™ [11]

0..1 A regular expression which denotes a pattern that shall be matched by the provided or subscribed Entities.

# Related

* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 5.5.13](/api-operations/common-behaviours/limiting-operations-to-local-scope.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.33](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — EntitySelector
