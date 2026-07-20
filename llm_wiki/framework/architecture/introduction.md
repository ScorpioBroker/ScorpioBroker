---
type: NGSI-LD Clause
title: "Introduction"
description: "The NGSI-LD API is intended to be primarily an API and does not define a specific architecture."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.3.1
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.3.1"
---

The NGSI-LD API is intended to be primarily an API and does not define a specific architecture. It is envisioned that
the NGSI-LD API can be used in different architectural settings and the architectural assumptions of the API are kept to
a minimum.
As it is not possible to elaborate all possible architectures in which the NGSI-LD API could be used, three prototypical
architectures are presented. The NGSI-LD API shall enable efficient support for all of them, i.e. the design decisions for
the NGSI-LD API take these prototypical architectures into consideration. A real system architecture utilizing the
NGSI-LD API can map to one, take elements from multiple or combine all of the prototypical architectures.
The NGSI-LD API implicitly defines two sets of Entities:
- the "current state";
- the "temporal evolution" (both the past and possibly future predictions). The NGSI-LD API is structured into a Core API and an optional Temporal API. The Core API manages the current state of Entities. The Temporal API is optional and manages the Temporal Evolution of Entities. Brokers that intend to implement the Temporal API should consider updating the Temporal Evolution of an Entity whenever the "current state" is modified via the Core API.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.3.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Introduction
