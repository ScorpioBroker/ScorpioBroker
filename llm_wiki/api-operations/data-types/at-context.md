---
type: NGSI-LD Data Type
title: "@context"
description: "When encoding NGSI-LD Entities, Context Source Registrations, Subscriptions and Notifications, as pure JSON-LD (MIME type \"application/ld+json\"), an array (flattened to a single string if necessary),"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.3
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.3"
---

When encoding NGSI-LD Entities, Context Source Registrations, Subscriptions and Notifications, as pure JSON-LD (MIME type "application/ld+json"), an array (flattened to a single string if necessary), containing a user @context where present, and the core @context (as described in clause 4.4) shall be included as a special member of the corresponding JSON-LD Object. Table 5.2.3-1 gives a precise definition of this special member.

Table 5.2.3-1: JSON-LD @context tagged member Name Data Type Restriction Cardinality Description @context URI, JSON Object, or JSON Array See [2], section 5.1. 0..1 JSON-LD @context.

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — @context
