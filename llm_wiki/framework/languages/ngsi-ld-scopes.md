---
type: NGSI-LD Clause
title: "NGSI-LD Scopes"
description: "An NGSI-LD Scope enables putting Entities into a hierarchical structure and restrict results of queries and notifications accordingly."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.18
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.18"
---

An NGSI-LD Scope enables putting Entities into a hierarchical structure and restrict results of queries and notifications
accordingly. The hierarchical structure is user-defined, e.g. according to (logical) location or organization. The use of
Scopes is optional and an Entity can be assigned to one or more Scopes at the same time. The Scope is represented as a
special scope Property that is non-reified in the normalized Entity representation and reified in the temporal
representation of an Entity. In the latter case, it is restricted to having the non-reified Temporal Properties createdAt,
modifiedAt and deletedAt as sub-Properties. There shall at most be one instance of the scope property per Entity. In case
multiple representations of the same Entity have to be merged, e.g. when combining the results of distributed queries,
the values of scope are merged. The value of scope is represented as a JSON array in case there is more than one Scope.
For the Temporal Evolution a given Scope is considered valid from the time it has been set until the time it has been
explicitly removed by an update or delete operation (for an example see annex C, clause C.5.16).
The grammar that encodes the syntax of the Scope is expressed in ABNF format [12]. It is described below (it has been
validated using https://github.com/ietf-tools/bap), and it shall be supported by implementations. The special string
"urn:ngsi-ld:null" (i.e. the NGSI-LD Null) shall be only used and only appear in case of deleted scopes.
Scope = [%x2F] ScopeLevel *(%x2F ScopeLevel) ; [/] ScopeLevel *(/ScopeLevel)
Scope =/ "urn:ngsi-ld:null" ; the literal string "urn:ngsi-ld:null"
ScopeLevel = unicodeLetter *ScopeLevelChar
ScopeLevelChar = unicodeNumber / unicodeLetter
ScopeLevelChar =/ %x5F ; _
EXAMPLE 1: /Madrid
EXAMPLE 2: Madrid
EXAMPLE 3: /Madrid/Gardens/ParqueNorte
EXAMPLE 4: /CompanyA/OrganizationB/UnitC

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.18](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Scopes
