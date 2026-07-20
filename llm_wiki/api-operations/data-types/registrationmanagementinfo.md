---
type: NGSI-LD Data Type
title: "RegistrationManagementInfo"
description: "This type represents the data to alter the default behaviour of a Context Broker when making a distributed operation request to a registered Context Source."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.34
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.34"
---

This type represents the data to alter the default behaviour of a Context Broker when making a distributed operation request to a registered Context Source. The supported JSON members shall follow the indications provided in Table 5.2.34-1. Brokers may override these recommendations.

Table 5.2.34-1: RegistrationManagementInfo data type definition Name Data Type Restrictions Cardinality Description cacheDuration String String representing a duration in ISO 8601 [17] format

| cacheDuration | String String representing a | | 0..1 | Minimal period of time which shall | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | duration in ISO 8601 | | | elapse between two consecutive context | | | | |
| | [17] format | | | information consumption operations (as | | | | |

0..1 Minimal period of time which shall
elapse between two consecutive context
information consumption operations (as
defined in clause 5.7) related to the
same context data will occur.
If the cacheDuration latency period has
not been reached, a cached value for
the entity or its attributes shall be
returned where available.
cooldown Number Greater than 0 0..1 Minimum period of time in milliseconds
which shall elapse before attempting to
make a subsequent forwarded request
to the same endpoint after failure.
If requests are received before the
cooldown period has expired, a timeout
error response for the registration is
automatically returned.
localOnly Boolean 0..1 If localOnly=true then distributed
operations associated to this Context
Source Registration will act only
on data held directly by the registered
Context
Source itself (see clause 4.3.6.4).
timeout Number Greater than 0 0..1 Maximum period of time in milliseconds
which may elapse before a forwarded
request is assumed to have failed.

# Related

* [Clause 4.3.6.4](/framework/architecture/limiting-cascading-distributed-operations.md)
* [Clause 5.7](/api-operations/consumption/context-information-consumption.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.34](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — RegistrationManagementInfo
