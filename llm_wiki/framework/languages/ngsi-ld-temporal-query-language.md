---
type: NGSI-LD Clause
title: "NGSI-LD Temporal Query Language"
description: "The NGSI-LD Temporal Query language shall be supported by implementations."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.11
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.11"
---

The NGSI-LD Temporal Query language shall be supported by implementations. It is intended to define predicates
which allow testing whether Temporal Properties of NGSI-LD Entities, Properties and Relationships, are within certain
temporal constraints. In particular it can be used to request historic Property values and Relationships that were valid
within the specified timeframe.
The following grammar defines the syntax that shall be supported:

timerel = beforeRel / afterRel / betweenRel beforeRel = "before" afterRel = "after" betweenRel = "between"

The points in time for comparison are defined as follows:

- A timeAt parameter, which shall represent the comparison point for the before and after relation and the starting point for the between relation. It shall be represented as DateTime (mandated by clause 4.6.3).
- An endTimeAt parameter, which is only used for the between relation and shall represent the end point for comparison. It shall be represented as DateTime (mandated by clause 4.6.3). The Temporal Property (see clause 4.8) to which the temporal query is to be applied can be specified by timeproperty. If no timeproperty is specified, the temporal query is applied to the default Temporal Property observedAt.
EXAMPLE 1: ?timerel=before
&timeAt=2017-12-13T14:20:00Z
EXAMPLE 2: ?timerel=between
&timeAt=2017-12-13T14:20:00Z
&endTimeAt=2017-12-13T14:40:00Z
&timeproperty=modifiedAt
EXAMPLE 3: Temporal query encoded as HTTP Query String, note that this is HTTP binding specific, to be
used via GET method, as defined in clause 6.18.3.2.
?timerel=between
&timeproperty=observedAt
&timeAt=2017-12-13T14:20:00Z


The semantics of the different temporal relations defined above is as follows, and shall be supported by compliant implementations:

- before relationship (production rule named beforeRel). For a Temporal Property to match, the value of the specified Temporal Property (or observedAt as default) has to be before the time specified by timeAt. The specified value is used as an exclusive bound in the Temporal Query;
- after relationship (production rule named afterRel). For a Temporal Property to match, the value of the specified Temporal Property (or observedAt as default) has to be after the time specified by timeAt. The specified value is used as an inclusive bound in the Temporal Query;
- between relationship (production rule named betweenRel). For a Temporal Property to match, the value of the specified Temporal Property (or observedAt as default) has to be after the time specified by timeAt and before the time specified by endTimeAt. In the Temporal Query, the value specified for the lower bound of the range is inclusive and the value specified for the upper bound of the range is exclusive. When resolving temporal queries, Entities which do not convey the target Temporal Property of the query shall be considered as non-matching.

# Related

* [Clause 4.6.3](/framework/restrictions/supported-data-types-for-values.md)
* [Clause 4.8](/framework/geo-temporal/temporal-properties.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.11](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Temporal Query Language
