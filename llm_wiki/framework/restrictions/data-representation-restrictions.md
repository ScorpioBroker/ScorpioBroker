---
type: NGSI-LD Clause
title: "Data Representation Restrictions"
description: "4.6.1 Supported text encodings NGSI-LD implementations shall support the UTF-8 text encoding format."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.6
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.6"
---

4.6.1 Supported text encodings

NGSI-LD implementations shall support the UTF-8 text encoding format. To avoid interoperability problems, applications shall provide JSON content encoded using UTF-8 and NGSI-LD systems shall also expose such JSON content using UTF-8.

4.6.2 Supported names

Even though the JSON serialization format allows inclusion of any character in the Unicode space, NGSI-LD restricts Entity Type names, Property names and Relationship names to the following ABNF grammar:

nameChar = unicodeNumber / unicodeLetter nameChar =/ %x5F ; _ name = unicodeLetter *nameChar

- unicodeNumber is any Unicode character that has Number as a Category [22]. With Unicode-capable regular expression (RegEx) parsers, such a character may be matched by \p{N}.
- unicodeLetter is any Unicode character that has Letter as a Category [22]. With Unicode-capable regular expression (RegEx) parsers, such a character may be matched by \p{L}. In order to avoid name clashing, names can be prefixed as specified by the following BNF grammar: prefix = unicodeLetter *nameChar name =/ prefix %x3A unicodeLetter *nameChar ; prefix:name When receiving a JSON-LD object with a name (Type, Property, Relationship) including characters different than those expressed above, implementations should raise an error of type BadRequestData.

4.6.3 Supported data types for Values

Compliant NGSI-LD implementations shall support the following data types for representing Values:

- All the JSON native data types as mandated by IETF RFC 8259 [6], section 3.
- All the GeoJSON Geometries [8] with the exception of GeometryCollection.
- DateTime string for encoding a timestamp, i.e. a calendar date together with a time of day, expressed in UTC, using the ISO 8601 [17] Complete Representation and in particular using the 'Extended Format', as described below: - The timestamp shall be a string containing Year, Month, Day, Hours, Minutes, Seconds and time zone components using the format YYYY-MM-DDThh:mm:ssZ as defined in ISO 8601 [17]. In this representation, the character "-" is used to separate the calendar date components, the character "T" is used to indicate the start of the time-of-day portion, the character ":" is used to separate the time-of-day components, and the trailing character "Z" is used to convey the time zone. - All the referred components shall appear in the string; reduced representations are not permitted. - The Seconds component may optionally contain a decimal fraction. In this case the string shall contain two integer digits, followed by a decimal point and then one or more fractional digits, up to a maximum of six. For example, YYYY-MM-DDThh:mm:ss.ssssssZ. In requests, also a comma instead of a decimal point may be used as separator for compatibility reasons.

NOTE 1: In previous versions of NGSI-LD, only the comma was supported as ISO 8601 [17] states that it is the preferred option. However, in practice the decimal point is more commonly used. - The trailing timestamp component shall contain the time zone related information and shall always be equal to the character "Z". Therefore, all timestamps shall be expressed in UTC.


- Date string for encoding a calendar date. It uses ISO 8601 [17] Complete Representation using the 'Extended Format', as described below: - It shall be a string containing Year, Month, Day components using the format YYYY-MM-DD as defined in ISO 8601 [17]. In this representation, the character "-" is used to separate the calendar date components. - All the referred components shall appear in the string; reduced representations are not permitted.
- Time string for encoding a local time expressed in UTC. It uses ISO 8601 [17] Complete Representation using the 'Extended Format', as described below: - It shall be a string containing Hours, Minutes and Seconds components using the format hh:mm:ssZ as defined in ISO 8601 [17]. In this representation, the character ":" is used to separate the local time components. - All the referred components shall appear in the string; reduced representations are not permitted. - The Seconds component may optionally contain a decimal fraction. In this case the string shall contain two integer digits, followed by a decimal point and then one or more fractional digits, up to a maximum of six. For example, hh:mm:ss.ssssssZ. In requests, also a comma instead of a decimal point may be used as separator for compatibility reasons.
- URI as mandated by ISO 8601 [17], Appendix A, production rule named 'URI'. Implementations may support additional data types different to those enumerated above, for instance:
- JSON-LD typed value (i.e. a string as the lexical form of the value together with a type, defined by an XSD base type or more generally an IRI).
- JSON-LD structured value (e.g. a set, a list).

NOTE 2: In previous versions of NGSI-LD, only the comma was supported as ISO 8601 [17] states that it is the preferred option. However, in practice the decimal point is more commonly used. - The string shall not contain expressions of the difference between local time and UTC. All representations shall be interpreted as being expressed in UTC.

4.6.4 Supported Content

In principle, context information providers can publish any kind of data serialized in JSON and encoded in UTF-8. Nonetheless, to avoid security problems caused by script injection attacks or other attack vectors, implementations should consider that the incoming data from a client may contain the following characters:

- %x3C; <
- %x3E; >
- %x22; "
- %x27; '
- %x3D; =
- %x3B; ;
- %x28; (
- %x29; ) When receiving entities (context information) encoded in JSON format and containing values that include the above characters, implementations should decide how to resolve the possible security problems that may be generated by the data. In all cases, implementations shall preserve the representation of the content of the values provided by the context information providers and return the original content when replying to context consumption requests.


If implementations decide to raise an error, the error shall be BadRequestData.

4.6.5 Supported data types for LanguageMaps

Compliant NGSI-LD implementations shall support the following data types for representing LanguageMaps:

- A JSON object consisting of a series of key-value pairs where the keys shall be JSON strings representing IETF RFC 5646 [28] language codes or the JSON-LD "@none" for representing default when no more specific language is found. and the values shall be JSON strings or arrays of JSON strings. Additionally, the languageMap encoding {"@none": "urn:ngsi-ld:null"} shall be used to represent an NGSI-LD Null during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12) and for representing deleted Language Properties in notifications and in temporal evolutions.

4.6.6 Ordering of Entities in arrays having more than one instance of the same Entity

Some services (batch operations, clauses 5.6.7, 5.6.8, 5.6.9 and 5.6.10) operate on an array of entities, as input, and if this array contains more than one instance of the same entity, then these entity instances shall come in chronological order, i.e. the first entity instance in the array shall be older than the second, the second shall be older than the third, etc. Without this assumption, there is no way for the request to be treated correctly, as the entity instances are often used for replacing or modifying the prior entity instance.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Data Representation Restrictions
