---
type: NGSI-LD Clause
title: "Pagination Behaviour"
description: "5.5.9.1 General Pagination Behaviour When resolving NGSI-LD Query operations, NGSI-LD Systems shall exhibit the behaviour described by the present clause: - Let Md be equal to the default maximum numb"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.9
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.9"
---

5.5.9.1 General Pagination Behaviour

When resolving NGSI-LD Query operations, NGSI-LD Systems shall exhibit the behaviour described by the present clause:

- Let Md be equal to the default maximum number of NGSI-LD Elements to be retrieved by the API during each query pagination iteration, as defined by the NGSI-LD implementation.
- Let Mc be equal to the maximum number of NGSI-LD Elements to be retrieved as requested by the NGSI-LD Client. If Mc is undefined then it shall be equal to Md.
- Let L be the maximum number of NGSI-LD Elements to be retrieved by the API during each query pagination iteration. L shall be equal to Mc.
- During query execution and for each pagination iteration, the query resolution mechanisms of the NGSI-LD System shall ensure that only up to a maximum of L NGSI-LD Elements are retrieved and returned to the NGSI-LD client, i.e. the maximum page size per iteration shall not overpass L. Nonetheless, implementations shall take care of not overpassing a maximum size of response payload body, which, in practice, implies that, under certain circumstances, the number of Elements retrieved per page can be lower than L.
- After the retrieval of each page (containing at most L NGSI-LD Elements) implementations shall check whether there are pending NGSI-LD Elements to be retrieved in the context of the current query. If that is the case, implementations shall flag NGSI-LD Clients of the existence of such NGSI-LD Elements. Ultimately, the flagging mechanisms used shall depend on each API binding but shall be present as mandated by the present clause.
- When flagging the existence of additional NGSI-LD Elements (pages) pending to be retrieved, generally, implementations shall provide NGSI-LD Clients pointers to get access to both the following page of NGSI-LD Elements and the previous one, according to the current pagination iteration.
- The pointer to the previous page of NGSI-LD Elements shall be included for all pagination iterations excepting the first one, as, obviously, there will be no previous NGSI-LD Elements.
- When the last page of NGSI-LD Elements is reached, only the pointer to the previous page shall be provided to NGSI-LD Clients, so that they can detect that no more NGSI-LD Elements are available.
- The pointers to NGSI-LD Elements shall contain all the parameters needed to allow NGSI-LD Clients to retrieve the next and previous page, without further interactions with the API.


While iterating over a set of pages, there might be changes in the target result set, due to additions or removals of NGSI-LD Elements occurring in between. Implementations may detect those situations and may warn NGSI-LD Clients appropriately. During pagination, the same @context shall be used. An attempt to use a different @context should result in an BadRequestData error.

5.5.9.2 Pagination option using limit and offset

The general pagination behaviour described in clause 5.5.9.1 only requires pointers to the following and previous pages, which can be implemented in a completely opaque way. Pagination may also be implemented in a transparent way, giving the Context Consumer more control over the process. In this case, the parameters limit and offset should be used, allowing the Context Consumer to adapt the size of the page using limit and jump to a desired set of elements in the results using offset.

5.5.9.3 Pagination with Entity maps

In the case of queries based on Entity maps, the set of Entities considered for the result is fixed with the initial query creating the Entity map. However, the Entity information itself can be dynamic, so filters shall be rechecked before returning results. In the case of split Entities, the Entities in the Entity map can only be considered as candidate Entities, since, at the time of Entity map creation, the Entities have not been aggregated and thus the filters could not be applied. This can only be done when preparing a page for pagination. Thus, Entities not or no longer fitting the query shall be removed from the Entity map during pagination. Pages shall always be filled to the maximum, as long as Entities are available. When using the previous link, the set of Entities on the previous page may not be the same as before, due to dynamic changes resulting in Entities no longer fulfilling the filter criteria of the query. As a result, the first page when going backward, and the last page, when going forward, may have less than the maximum number of Entities.

# Related

* [Clause 5.5.9.1](/api-operations/common-behaviours/general-pagination-behaviour.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.9](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Pagination Behaviour
