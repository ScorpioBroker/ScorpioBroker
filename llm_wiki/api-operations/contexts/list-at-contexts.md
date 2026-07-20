---
type: NGSI-LD Operation
title: "List @contexts"
description: "5.13.3.1 Description With this operation a client can obtain a list of URLs that represent all of the @contexts stored in the local context store of the broker."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.13.3
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.13.3"
---

5.13.3.1 Description

With this operation a client can obtain a list of URLs that represent all of the @contexts stored in the local context store of the broker. Each URL can be used to download the corresponding @context, and, in case the @context's kind is "Cached", it shall be the original URL the broker downloaded the @context from. In case a details flag is set to true, the client obtains a list of JSON objects, each representing information (metadata) about an @context currently stored by the broker. Each JSON object contains information about the @context's original URL (if any), its local identifier in the broker's storage, its kind ("Cached", "Hosted" and "ImplicitlyCreated"), its creation timestamp, its expiry date, and additional optional information.

5.13.3.2 Use case diagram

A client can list all @contexts stored within an NGSI-LD system as shown in Figure 5.13.3.2-1.


Figure 5.13.3.2-1: List @contexts use case

5.13.3.3 Input data

- A kind filter indicating the kind of stored @contexts that are to be included in the output list. Currently, possible kinds are "Cached", "Hosted" and "ImplicitlyCreated" (optional).
- A boolean details flag indicating that detailed JSON objects representing metadata about the stored @contexts instead of simple URLs are requested (optional).

5.13.3.4 Behaviour

The broker shall provide a URL or JSON object for each @context currently stored in the internal broker's storage, that match the filter. If no filter is specified, all kinds are included.

5.13.3.5 Output data

A list of URLs, or a list of resulting JSON objects containing the following fields:

- URL;
- localId;
- kind;
- createdAt;
- expiresAt [OPTIONAL];
- lastUsage [OPTIONAL];
- numberOfHits [OPTIONAL, number of times the @context was found in the storage];
- extraInfo [OPTIONAL, used by implementations to report any kind of custom information].

# Related

* [HTTP: Resource: jsonldContexts/](/http-binding/resource-jsonldcontexts.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.13.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — List @contexts
