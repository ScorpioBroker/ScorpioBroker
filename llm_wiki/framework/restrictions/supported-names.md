---
type: NGSI-LD Clause
title: "Supported names"
description: "Even though the JSON serialization format allows inclusion of any character in the Unicode space, NGSI-LD restricts Entity Type names, Property names and Relationship names to the following ABNF gramm"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.6.2
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.6.2"
---

Even though the JSON serialization format allows inclusion of any character in the Unicode space, NGSI-LD restricts Entity Type names, Property names and Relationship names to the following ABNF grammar:

nameChar = unicodeNumber / unicodeLetter nameChar =/ %x5F ; _ name = unicodeLetter *nameChar

- unicodeNumber is any Unicode character that has Number as a Category [22]. With Unicode-capable regular expression (RegEx) parsers, such a character may be matched by \p{N}.
- unicodeLetter is any Unicode character that has Letter as a Category [22]. With Unicode-capable regular expression (RegEx) parsers, such a character may be matched by \p{L}. In order to avoid name clashing, names can be prefixed as specified by the following BNF grammar: prefix = unicodeLetter *nameChar name =/ prefix %x3A unicodeLetter *nameChar ; prefix:name When receiving a JSON-LD object with a name (Type, Property, Relationship) including characters different than those expressed above, implementations should raise an error of type BadRequestData.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.6.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Supported names
