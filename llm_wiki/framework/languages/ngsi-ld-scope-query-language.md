---
type: NGSI-LD Clause
title: "NGSI-LD Scope Query Language"
description: "The NGSI-LD Scope Query Language shall be supported by implementations."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.19
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.19"
---

The NGSI-LD Scope Query Language shall be supported by implementations. It is intended to select only those Entities that are within the specified Scope(s). Scopes are specified as a disjunction of elements, where each element can either directly be a Scope or a conjunction of multiple Scopes. The "+" can be used as a wildcard to match a single scope level within a Scope. The "#" that can be added at the end of a Scope specification serves as a wildcard, which matches the given scope and the whole hierarchy of scopes below. The "/#" matches any non-empty scope, i.e. only explicitly specified scopes. The logical operators are the same as in the NGSI-LD Query Language specified in clause 4.9. As a disjunction of Scopes can also be seen as a list, a comma can be used as an alternative representation of the or operator. For logical and grouping parenthesis are needed.

ScopesQ = OrScopeQ *(orOp OrScopeQ) ; OrScopeQ|OrScopeQ
ScopesQ =/ %x2F %23 ; / #
OrScopeQ = %x28 ScopeQ *(andOp ScopeQ) %x29 ; (ScopeQ;ScopeQ)
OrScopeQ =/ ScopeQ *1(%x2F %23) ; ScopeQ / #
andOp = %x3B ; ;
orOp = %x7C / %x2C ; | ,
ScopeQ = %x2F ScopeQLevel *(%x2F ScopeQLevel) ; /ScopeQLevel *(/ScopeQLevel)
ScopeQLevel = unicodeLetter *ScopeQLevelChar


ScopeQLevel =/ %x2B ; + ScopeQLevelChar = unicodeNumber / unicodeLetter ScopeQLevelChar =/ %x5F ; _

EXAMPLE 1: Scope /Madrid:
/Madrid
EXAMPLE 2: Scope /Madrid/Gardens and the whole scope tree below:
/Madrid/Gardens/#, e.g. matches the following Scopes:
/Madrid/Gardens, /Madrid/Gardens/ParqueNorte,
/Madrid/Gardens/ParqueNorte/Parterre1, /Madrid/Gardens/ParqueSur
EXAMPLE 3: Scopes /Madrid/Gardens/ParqueNorte and /Madrid/Sights/ParqueNorte, or any other Scope with
Madrid as first scope level and ParqueNorte as third scope level:
/Madrid/+/ParqueNorte
EXAMPLE 4: Scope /Madrid/Districts and /CompanyA:
(/Madrid/Districts;/CompanyA)
EXAMPLE 5: Scope (/Madrid/Districts and /CompanyA) or /CompanyB:
(/Madrid/Districts;/CompanyA)|CompanyB
Alternative Representation:
(/Madrid/Districts;/CompanyA),CompanyB

# Related

* [Clause 4.9](/framework/languages/ngsi-ld-query-language.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.19](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Scope Query Language
