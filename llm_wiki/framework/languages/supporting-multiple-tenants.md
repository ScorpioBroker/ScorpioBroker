---
type: NGSI-LD Clause
title: "Supporting Multiple Tenants"
description: "The concept of a Tenant is that a user or group of users utilizes a single instance of an NGSI-LD system (Context Source or Context Broker) in isolation from other users or groups of users of the same"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.14
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.14"
---

The concept of a Tenant is that a user or group of users utilizes a single instance of an NGSI-LD system (Context
Source or Context Broker) in isolation from other users or groups of users of the same instance, which are
considered to be different Tenants. Thus a multi-tenant NGSI-LD system is a system where a single software instance
is used by different users or groups of users, the Tenants, where any information related to one Tenant (e.g.
Entities, Subscriptions, Context Source Registrations) are only visible to users of the same Tenant, but
not to users of a different Tenant. Typically, multi-tenancy is used together with an access control mechanism,
enforcing the isolation of Tenants, however access control and other security-related aspects are out-of-scope of the
present document.
The NGSI-LD API optionally enables multi-tenant systems. To support this, Tenant information can be optionally
specified in NGSI-LD API operations. The operation then only applies to the targeted Tenant. As all information of
one Tenant is isolated from other Tenants, the NGSI-LD API operations for managing, retrieving and subscribing
to entity information, but also any context source related operations only apply to the information of the specified
Tenant in isolation and never have any effect on the information of other Tenants.
As the support and use of Tenants is optional, any operation not explicitly specifying a Tenant targets a default
Tenant, which always exists. NGSI-LD systems not supporting multiple Tenants should raise an error of type
NoMultiTenantSupport if a Tenant is specified. To enable Context Sources to be part of tenant-based
distributed or federated systems, Tenant information can optionally be specified in Context Source
Registrations. When contacting the respective Context Sources, the Tenant information from the
Context Source Registration has to be used. If no Tenant information is present in the Context
Source Registration, no Tenant information is to be used and thus the default Tenant is targeted on the
registered Context Source. This enables integrating Context Sources not supporting multi-tenancy in a
distributed system with a Tenant-based Context Broker or integrating local Tenants in a federated system
using a different Tenant.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.14](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Supporting Multiple Tenants
