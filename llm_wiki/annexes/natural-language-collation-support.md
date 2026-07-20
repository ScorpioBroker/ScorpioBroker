---
type: Reference
title: "Natural Language Collation Support"
description: "G.2.0 Foreword All strings within an NGSI-LD system are defined and sorted as a sequence of Unicode characters."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-G.2
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "G.2"
---

G.2.0 Foreword

All strings within an NGSI-LD system are defined and sorted as a sequence of Unicode characters. As such there is no simple collation mechanism to query entities ignoring case, diacritic marks or matching diphthong single letters such as

the German "ö" to also match with "oe".

Many databases support a degree of natural language support, in general collation support will always depend upon the underlying database and as such will vary from implementation to implementation. This therefore and cannot be standardized and exposed as part of the context information management API. Furthermore, collation is slow and

processor intensive, and for massive systems is better achieved using a separate index.


For systems that require it, this clause proposes a mechanism as an extension to a NGSI-LD Context Broker which can be modified and used to offer collation support to the natural language attributes found within context entities where necessary through creating, querying and maintaining an additional property of a property for collated attributes.

G.2.1 Maintain collations as metadata

- Create a subscription on the attribute (e.g. name)
- Create a simple microservice to add/upsert a name.collate property-of-a-property using a simple function to strip all diacritic marks - for example: str.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLower() Other substitutions could be made where local spelling rules vary (for example different for German ö = oe).

G.2.2 Route language sensitive queries via a proxy

Create a simple forwarding proxy around the NGSI-LD system. For any urls with a q param (and a collate flag) run a
clean-up of the q param and amend the query string:
The following request on the proxy:
GET /ngsi-ld/v1/entities/?type=Building&q=name==%22Schöne%20Grüße%22&collate=name
is altered on the fly and is sent to the NGSI-LD system as shown:
GET /ngsi-ld/v1/entities/?type=Building&q=name.collate==%22schoene%20gruesse%22
Once again, the substitutions to make to the query string will depend on the rules of the natural language to be
supported.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause G.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Natural Language Collation Support
