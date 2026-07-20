---
type: NGSI-LD Clause
title: "Pagination with Entity maps"
description: "In the case of queries based on Entity maps, the set of Entities considered for the result is fixed with the initial query creating the Entity map."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.9.3
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.9.3"
---

In the case of queries based on Entity maps, the set of Entities considered for the result is fixed with the initial query creating the Entity map. However, the Entity information itself can be dynamic, so filters shall be rechecked before returning results. In the case of split Entities, the Entities in the Entity map can only be considered as candidate Entities, since, at the time of Entity map creation, the Entities have not been aggregated and thus the filters could not be applied. This can only be done when preparing a page for pagination. Thus, Entities not or no longer fitting the query shall be removed from the Entity map during pagination. Pages shall always be filled to the maximum, as long as Entities are available. When using the previous link, the set of Entities on the previous page may not be the same as before, due to dynamic changes resulting in Entities no longer fulfilling the filter criteria of the query. As a result, the first page when going backward, and the last page, when going forward, may have less than the maximum number of Entities.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.9.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Pagination with Entity maps
