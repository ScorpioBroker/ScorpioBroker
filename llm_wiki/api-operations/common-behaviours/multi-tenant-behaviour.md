---
type: NGSI-LD Clause
title: "Multi-Tenant Behaviour"
description: "If a Tenant is specified for an NGSI-LD operation, the operation shall only be applied to information related to the specified Tenant."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.10
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.10"
---

If a Tenant is specified for an NGSI-LD operation, the operation shall only be applied to information related to the
specified Tenant. If no Tenant is specified, the operation shall apply to the implicitly existing default Tenant. If a
Tenant is explicitly specified, but the system implementing the NGSI-LD API does not support multi-tenancy, an
error of type NoMultiTenantSupport should be raised.
In case an operation applies to a Tenant, this information shall also be provided in the response to the operation. This
also applies to notifications sent as a result of subscriptions (clauses 5.8 and 5.11).
A Tenant is represented in form of a String. How the Tenant is specified for an API operation is protocol binding
specific. How Tenants are created, is implementation-specific.
One implementation option is to support the implicit creation of Tenants. This means that a Tenant is implicitly
created when an NGSI-LD operation for creating information targets a new Tenant; this is the case for:
- Create Entity (clause 5.6.1).
- Batch Entity Creation (clause 5.6.7).
- Create or Update Temporal Evolution of an Entity (clause 5.6.11).
- Create Subscription (clause 5.8.1).
- Register Context Source (clause 5.9.2).
- Create Context Source Registration (clause 5.11.2). All other NGSI-LD operations, e.g. for retrieving, updating, appending or deleting information that target a non-existing Tenant should raise an error of type NonexistentTenant. If the system implementing the NGSI-LD API does not support multiple Tenants, the attempt to register a Context Source with Tenant information in the Context Source Registration should also result in an error of type NoMultiTenantSupport.

# Related

* [Clause 5.11.2](/api-operations/csource-subscription/create-context-source-registration-subscription.md)
* [Clause 5.6.1](/api-operations/provision/create-entity.md)
* [Clause 5.6.11](/api-operations/provision/create-or-update-upsert-temporal-evolution-of-an-entity.md)
* [Clause 5.6.7](/api-operations/provision/batch-entity-creation.md)
* [Clause 5.8.1](/api-operations/subscription/create-subscription.md)
* [Clause 5.9.2](/api-operations/registration/register-context-source.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.10](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Multi-Tenant Behaviour
