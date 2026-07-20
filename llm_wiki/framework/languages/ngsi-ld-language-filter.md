---
type: NGSI-LD Clause
title: "NGSI-LD Language Filter"
description: "The NGSI-LD Language Filter shall be supported by implementations."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.15
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.15"
---

The NGSI-LD Language Filter shall be supported by implementations. It is intended to form a mechanism which allows just one matching string value of LanguageProperties of NGSI-LD Entities to be converted to an NGSI-LD Property, where the value will be a string for the specified natural language.


The following grammar defines the syntax that shall be supported by the filter:
lang = langtag
Where the langtag is defined according to the rules as mandated by IETF RFC 5646 [28], and IETF RFC 3282 [29]. If
the Context Broker cannot serve any matching language, it shall default to any supported language. This behaviour
can be triggered by specifying lang="*" in the filter (see example 3).
In any case, the attribute in question shall be augmented with an additional non-reified subproperty lang indicating the
actual language returned.
EXAMPLE 1: Specified natural language - return LanguageProperties as strings in English only.
lang="en"
EXAMPLE 2: Multiple natural languages with no ranked preference - return LanguageProperties as strings in
either Swiss French or French.
lang="fr-CH,fr"
EXAMPLE 3: Wildcard - return LanguageProperties as a string in any supported language.
lang="*"
EXAMPLE 4: Quality value ranking - return all LanguageProperties as a string in Swiss French or French with
no ranked preference, fallback to English as a second choice and finally fallback to any other
supported language.
lang="fr-CH,fr;q=0.9,en;q=0.8,*;q=0.5"

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.15](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Language Filter
