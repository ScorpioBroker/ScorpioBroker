# Findings: Linked Entity Retrieval — `objectType` vs `local` (ETSI Consumption/Entity cluster "A")

**Date:** 2026-07-19  
**Branch:** `testfest`  
**Spec reference:** ETSI GS CIM 009 V1.9.1 (2025-07) — `gs_CIM009v010901p.pdf` (repo root)  
**Test suite:** `ContextInformation/Consumption/Entity`, failures grouped as cluster **A**
(6 tests: `018_22_03/04/05`, `019_30_03/04/05`)

## The question

When Linked Entity Retrieval (`join=inline` / `join=flat`) is requested, **which relationships
does the broker follow to pull in the linked entity?**

- The **test suite** expects a relationship that targets a locally stored entity to be followed
  **even when it has no `objectType`** and even without `local=true`.
- **Scorpio** only follows a relationship without `objectType` when the request is
  `local=true`; otherwise a relationship must carry `objectType` to be joined.

The proposed characterization under review was: *"objectType-free relationships are only
followed if `local=true`."* This note records whether the spec actually says that.

## What Scorpio currently does (code)

The `local` request parameter is the switch that decides whether a bare (no-`objectType`)
relationship is followed:

- `QueryManager/.../repository/QueryDAO.java:960-985` — `generateJoinQuery` only requires
  `Y ? 'hasObjectType'` (relationship must contain an `objectType`) in the **non-local** branch;
  the `localOnly` branch drops that requirement.
- `QueryManager/.../services/QueryService.java` — `inlineAttrib` / `flatAddAttrib`: the
  `localOnly` path follows every relationship target present in the cache; the non-local path
  is gated on `attribMap.containsKey(NGSI_LD_OBJECT_TYPE)`.
- `Commons/.../datatypes/terms/QQueryTerm.java:991` — same `hasObjectType` gate on the linked-q
  SQL for the non-local path.

So "follow bare relationships only when `local=true`" is an accurate description of **Scorpio's
implementation**. The question is whether the spec mandates it.

## Spec evidence

Every occurrence of `objectType`, "annotated", "Linked Entity retrieval", "Internal Linked
Entity", plus clause 5.5.13 (local scope) and the behaviour clauses 5.7.1.4 / 5.7.2.4 was
reviewed. Findings:

### 1. No clause links relationship-following to the `local` request parameter

Clause **5.5.13 "Limiting operations to local scope" (p.157)** defines local scope purely as
*"only execute … based on information available in the Context Source or Context Broker directly
targeted by the request"* — i.e. do not cascade to registered Context Sources. **None of the
Linked Entity Retrieval clauses reference the local parameter at all.** The exact formulation
"objectType-free relationships are only followed if `local=true`" does **not** appear in the
spec; that coupling is Scorpio's implementation choice, not a spec requirement.

### 2. The representation clauses support the TEST suite ("locally stored OR annotated")

- **4.5.23.1 (p.77):** *"Only Relationships targeting a **locally stored** Entity **or**
  Relationships annotated with an objectType whose object is an Internal Linked Entity are
  considered to be retrievable in this manner."*
- **4.5.4 simplified representation (p.56, p.58):** repeated twice —
  *"Any Relationship which targets an Entity **stored locally or** includes an objectType
  Attribute is returned as …"*
- **"Internal Linked Entity" definition (p.25):** *"Linked Entity that exists within the current
  NGSI-LD **system**."*

Read together: "locally stored" is a property of the **data** (the target entity is in this
broker's store), not of the **request**. The request-scoped concept is consistently named
"local scope" everywhere else in the document. Under this reading the broker should follow a
bare relationship whenever its target is in the local store — which is exactly what the tests
assert, and exactly what Scorpio does **only** under `local=true`.

### 3. The behaviour clauses support SCORPIO ("annotated" only)

- **Retrieve Entity 5.7.1.4 (p.198-199)** and **Query Entities 5.7.2.4 (p.204)** describe the
  join exclusively in terms of **"annotated"** relationships:
  *"If inline Linked Entity retrieval is specified, and any of the returned Attributes
  corresponds to an **annotated** Relationship, then … an entity sub-Property shall be
  included …"*; the flattened text appends only entities *"defined by an **annotated**
  Relationship."*
- **"annotated"** is never formally defined, but the only annotation the spec ever attaches to a
  relationship is 4.5.23.1's *"annotated with an objectType."* So "annotated Relationship"
  reads as "relationship carrying an `objectType`", regardless of local scope — matching
  Scorpio's non-local behaviour.

## Conclusion

**The spec is internally inconsistent.**

| Reading | Clauses | Behaviour | Who implements it |
|---|---|---|---|
| "locally stored **OR** objectType-annotated" | 4.5.23.1 (p.77), 4.5.4 (p.56/58) | follow bare relationship to any locally-stored target | **ETSI test suite** |
| "**annotated** (objectType) only" | 5.7.1.4 (p.198), 5.7.2.4 (p.204) | follow only objectType-annotated relationships | **Scorpio** (non-local) |

- The precise claim "bare relationships are only followed if `local=true`" is **not stated in
  the spec** — that is Scorpio's own mapping. `local=true` is an added relaxation the spec
  neither requires nor forbids.
- The test suite implements the **representation-clause** reading; Scorpio implements the
  **behaviour-clause** reading. Both are defensible against the letter of the document.

Therefore cluster A is **not a plain Scorpio bug**: it stems from a genuine contradiction
between 4.5.23.1 / 4.5.4 and 5.7.1.4 / 5.7.2.4. Fixing Scorpio to pass A means adopting the
representation-clause reading (follow bare relationships to locally-stored targets even without
`local=true`), which is a behaviour change with distributed-query implications, not a
clarification.

## Recommendation

1. **Do not change Scorpio's join gating solely to satisfy cluster A.** The behaviour clauses
   (5.7.1.4 / 5.7.2.4) back the current implementation.
2. **Raise the contradiction with ETSI** — either an issue against the NGSI-LD test suite, or a
   Change Request asking CIM to define "annotated Relationship" and reconcile 4.5.23.1 / 4.5.4
   with 5.7.1.4 / 5.7.2.4.
3. If a product decision is later made to adopt the representation-clause reading, the change is
   localized to the non-local branches listed under "What Scorpio currently does" — but it must
   be regression-tested against the DistributedOperations suite and CommonBehaviours (33/33),
   since it widens which relationships trigger a linked lookup in the distributed path.

## Related cluster B (same tests' sibling issue — for completeness)

Cluster **B** (8 tests) is `objectType` being **echoed** in join responses while no suite
expectation ever contains `objectType`. This is defensible as test overreach: **4.5.3.2 (p.52)**
lists `objectType` as an *Optional* member of the normalized relationship representation, so
echoing what the creator stored is spec-legal. If ever aligned to the suite, strip
`NGSI_LD_OBJECT_TYPE` in join **output only** (safest reading).
