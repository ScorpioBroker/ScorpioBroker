---
type: Reference
title: "Localization of Dates, Currency formats, etc."
description: "G.3.0 Foreword Context data entities are designed to be interoperable and therefore all dates are held as UTC dates, all currency amounts are held as JSON numbers (with the unitCode property-of-a-prop"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-G.3
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "G.3"
---

G.3.0 Foreword

Context data entities are designed to be interoperable and therefore all dates are held as UTC dates, all currency amounts are held as JSON numbers (with the unitCode property-of-a-property available to hold the currency), etc. Localization should not occur within the context data entities themselves. Offering fully localized responses is not a concern of the NGSI-LD API. If localization support is necessary, a simple proxying a conversion mechanism could be used to amend the context data received from the NGSI-LD system before being passed to a third party system for display.

G.3.1 Localizing Dates

For example, if a system needs to display DateTime data in Islamic Date format
The following request on the proxy:
GET /ngsi-ld/v1/entities/urn:ngsi-ld:Event:XXX?attrs=date&format=simplified
is forwarded unaltered and is sent to the NGSI-LD system as shown:
GET /ngsi-ld/v1/entities/urn:ngsi-ld:Event:XXX?attrs=date&format=simplified
The response from the NGSI-LD system is always in UTC format:

{"date": "2020-09-28T17:13:39+02:00"}


And the proxy can be used to update this to the desired format:

{"date": "11 Safar, 1442 1:13:39PM"}

Using an internationalization script such as the following:

new Intl.DateTimeFormat("en-u-ca-islamic", {day: 'numeric', month: 'long',weekday: 'long',year : 'numeric'}).format(date);

It should be noted that post-localization, the transformed date is no longer valid NGSI-LD.


Annex H (informative): Suggested actuation workflows

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause G.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Localization of Dates, Currency formats, etc.
