---
type: Reference
title: "HTTP Request"
description: "GET /ngsi ld/v1/temporal/entities/?type=Vehicle&q=brandName!=Mercedes&attrs=speed&timerel=between&timeAt=2018- 08-01T12:00:00Z&endTimeAt=2018-08- 01T13:00:00Z&aggrMethods=max,avg&aggrPeriodDuration=PT"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5.14.2
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5.14.2"
---

GET /ngsi ld/v1/temporal/entities/?type=Vehicle&q=brandName!=Mercedes&attrs=speed&timerel=between&timeAt=2018-

08-01T12:00:00Z&endTimeAt=2018-08-

01T13:00:00Z&aggrMethods=max,avg&aggrPeriodDuration=PT4M&format=aggregatedValues

Accept: application/ld+json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.5.14.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — HTTP Request
