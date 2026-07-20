---
type: Reference
title: "Route language sensitive queries via a proxy"
description: "Create a simple forwarding proxy around the NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-G.2.2
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "G.2.2"
---

Create a simple forwarding proxy around the NGSI-LD system. For any urls with a q param (and a collate flag) run a
clean-up of the q param and amend the query string:
The following request on the proxy:
GET /ngsi-ld/v1/entities/?type=Building&q=name==%22Schöne%20Grüße%22&collate=name
is altered on the fly and is sent to the NGSI-LD system as shown:
GET /ngsi-ld/v1/entities/?type=Building&q=name.collate==%22schoene%20gruesse%22
Once again, the substitutions to make to the query string will depend on the rules of the natural language to be
supported.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause G.2.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Route language sensitive queries via a proxy
