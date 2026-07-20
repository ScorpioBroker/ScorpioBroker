---
type: NGSI-LD Clause
title: "Supported data types for Values"
description: "Compliant NGSI-LD implementations shall support the following data types for representing Values: - All the JSON native data types as mandated by IETF RFC 8259 [6], section 3."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.6.3
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.6.3"
---

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

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.6.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Supported data types for Values
