---
type: NGSI-LD Clause
title: "NGSI-LD EntityMap Representation"
description: "The EntityMap representation is used by Context Brokers to ensure unity when querying across distributed operations."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.25
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.25"
---

The EntityMap representation is used by Context Brokers to ensure unity when querying across distributed
operations. It is an active mapping of Context Source Registrations to Entities which are relevant to an
ongoing Context Information Consumption request (see clause 4.3.6.7). The EntityMap representation shall be a JSON
LD object containing the following members:
Mandatory
- "id": whose value shall be the URI that identifies the attribute.
- "type": the fixed value "EntityMap".
- "expiredBy": a DateTime string (as defined in clause 4.6.3) for encoding a timestamp indicating the time at which the EntityMap can no longer be used.

Optional

- "@context": a JSON-LD @context as described in clause 4.4. Output Only
- "entityMap": a JSON-LD index map holding a unique list of entity ids (URIs) each of which lists the registrations that were fired by the previous request and successfully returned data.
- "linkedMaps": a JSON-LD index map holding a unique list of Context Source Registrations ids (URIs) each of which lists the entityMap used by a Context Source.

# Related

* [Clause 4.3.6.7](/framework/architecture/querying-and-retrieving-distributed-entities-as-unitary-operations.md)
* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.6.3](/framework/restrictions/supported-data-types-for-values.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.25](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD EntityMap Representation
