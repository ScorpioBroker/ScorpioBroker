# Fix Plan: ETSI Consumption/Entity clusters C, F, G

**Date:** 2026-07-19  
**Branch:** `testfest`  
**Spec reference:** ETSI GS CIM 009 V1.9.1 (2025-07) — `gs_CIM009v010901p.pdf` (repo root)  
**Baseline:** `ContextInformation/Consumption/Entity` = 42 failures (identical to pre-refactor
baseline). C + F + G together clear **20 failing tests** (16 in this suite + 4 temporal that C/G
also fix). All three are independent and can land as separate commits.

Test-run recipe and how the failure list was extracted: repo `CLAUDE.md` §"Running the ETSI
NGSI-LD test suite locally". Results dir of the analyzed run: `results-cons-entity-0719`.

---

## Cluster C — POST-body `joinLevel` default is wrong (4 tests)

### Failing tests
`019_24_01`, `019_24_02`, `019_24_03`, `019_24_04` (`POST /entityOperations/query` with
`join` in the body but no `joinLevel`).

### Symptom
`join` in the POST body is silently ignored — no linked entities returned.

### Spec basis
- **5.2.23 Query datatype (p.126):** `joinLevel` — *"Number, Positive Integer, 0..1 — Depth of
  Linked Entity retrieval to apply. **Default is 1.** Only applicable if join parameter is
  'flat' or 'inline'."* The POST body **is** this datatype.
- Same wording in the notification-params table (p.119). HTTP binding table (p.283) restates
  "only applicable if join parameter is present".

### Root cause
The GET paths already default correctly — `QueryController.java:586-595`:
```java
if (joinLevelInput == null) {
    joinLevel = (join == null) ? 0 : 1;   // join present, no joinLevel -> depth 1
} else {
    joinLevel = joinLevelInput;
}
```
The two POST-body controllers hardcode `null -> 0`, so every join gate (`joinLevel > 0`) stays
shut:
- `QueryManager/.../controller/EntityOperationsQueryController.java:188`
  → `int joinLevel = joinLevelObj == null ? 0 : (int) joinLevelObj;`
- `HistoryQueryManager/.../controller/HistoryOperationsController.java:210`
  → same line (latent; no Entity-suite test hits it, but the TemporalEntity suite likely does).

### Fix
Mirror the GET defaulting in both controllers:
```java
int joinLevel = joinLevelObj == null ? (join == null ? 0 : 1) : (int) joinLevelObj;
```
One line each. No other call sites.

### Risk
Minimal. Only changes behaviour when a POST body has `join` without `joinLevel` — previously a
no-op, now spec-correct. No Postman-gate exposure (checked).

### Verification
`019_24_01..04` → green; rerun full Consumption/Entity for no regressions; TemporalEntity POST
query suite; Postman gate (fresh stack).

---

## Cluster F — pick/omit projection grammar not validated (12 tests)

### Failing tests
`019_18_04..09` (6) and `018_18_04..09` (6): malformed `pick`/`omit` values return **200**
instead of **400 BadRequestData**. One (`018_18_04`) currently returns 404. Cases:
`id;name`, `id,locatedAt{name` (unclosed), `id,locatedAt{{name}` (double brace),
`id,,name` (consecutive separators), `id,locatedAt{,name}` (leading special),
`id,locatedAt{}` (empty expression).

### Spec basis
- **Clause 4.21 Attribute Projection Language (p.101):**
  ```
  orOp             = %x7C / %x2C                         ; |  ,
  ProjectionTerm   = AttrName *1(LinkedEntityTerm) *(orOp ProjectionTerm)
  LinkedEntityTerm = %x7B ProjectionTerm %x7D           ; { ProjectionTerm }
  ```
- **AttrName from clause 4.9 (p.85):**
  ```
  AttrName = unicodeLetter *TermChar
  TermChar = unicodeLetter / unicodeNumber / %x5F       ; letter, digit, _
  ```
  (`unicodeLetter` = `\p{L}`, `unicodeNumber` = `\p{N}`.)

### Root cause
`Commons/.../tools/QueryParser.java:548-591` `parseProjectionTerm` is a char-scanner with **zero
validation**. An unbalanced `}` even does `current = current.getParent()` down to `null`, which
would NPE (500) on the next token. It is the single parser behind **all** call sites
(GET query, GET retrieve, POST entityOperations body, temporal paths), so one fix is uniform.

### Fix — validation to add inside the existing loop
Throw `ResponseException(ErrorType.BadRequestData, …)` (suite asserts BadRequestData for all 12):

1. **Brace depth counter.** `}` at depth 0 → error; input ends with depth > 0 → error
   (*unclosed brace*, `_05`). Removes the NPE hazard too.
2. **On `{`:** current token must be non-empty → catches `{{` (*double braces*, `_06`) and a
   leading `{`.
3. **On `,` / `|`:** token must be non-empty **unless** the previous char closed a brace
   (a `justClosed` flag, so `a{b},c` stays legal) → catches `id,,name` (`_07`) and
   `{,name}` (`_08`).
4. **On `}`:** token non-empty unless `justClosed` (so nested `a{b{c}}` works) → catches
   `locatedAt{}` (`_09`).
5. **After a `}`,** only `,` / `|` / `}` / end may follow → rejects `a}b`.
6. **Completed tokens must match AttrName** — regex `\p{L}[\p{L}\p{N}_]*` — with two pragmatic
   extensions Scorpio already relies on elsewhere: allow the full-IRI form (contains `:`,
   validate as URI) since expanded IRIs are accepted everywhere, and allow `@` for `@none`-style
   members. `id;name` (`_04`) matches neither → 400.
7. **Empty/blank input** after trim → error (spec: empty array not allowed).

### Drive-by fix (same area, real bug)
`EntityOperationsQueryController.java:264`: the `omit`-as-`List` branch passes **`pickTerm`**
into the parser instead of `omitTerm`. Two tokens.

### Risk — Postman gate (checked)
Every `pick`/`omit` value in `api-test.json` is simple names + `,` + balanced `{}` sub-terms
(e.g. `pick=id,type,soilType,observation{humidity,windSpeed}`, some with a trailing `%20` that
the existing `trim()` handles). No dots, URIs, or odd characters → strict charset is gate-safe.

### Verification
Add ~8 unit cases to Commons `QueryParamParserTest` (each invalid pattern + the legal
nested/`justClosed` shapes). Rerun `019_18` / `018_18` (12 → green; the 404 case flips because
validation now fires before entity lookup). Full Consumption/Entity for regressions. Postman
gate.

---

## Cluster G — invalid `options` / `format` value → wrong error type (4 tests)

### Failing tests
`018_16_01..04`: invalid `format` or `options` value returns **400 with type
`BadRequestData`**; suite (tagged `6_3_7`, `6_3_20`) wants type **`InvalidRequest`**. Status
code (400) is already correct — only the ProblemDetails `type` URI is wrong.

### Spec basis
- Suite asserts `ERROR_TYPE_INVALID_REQUEST` in `018_16` and in temporal `021_20` — no
  suite-internal conflict.
- Matches the 6.3.20 convention the query-param refactor already adopted for unknown params.

### Root cause
`Commons/.../tools/HttpUtils.java:1526-1553` `parseOptionsAndFormat` throws
`ErrorType.BadRequestData` for a value not in `ALLOWED_OPTIONS`:
- line **1534** (options branch)
- line **1544** (format branch)

### Fix
Both → `ErrorType.InvalidRequest`. Status stays 400; only the `type` URI changes. All callers
(query, retrieve, batch query, temporal) get it uniformly.

### Risk — Postman gate (checked)
Zero requests in `api-test.json` send an invalid `options`/`format` value, and nothing asserts
on that error text. No gate exposure.

### Verification
`018_16_01..04` (+ temporal `021_20`) → green. Postman gate (fresh stack).

---

## Suggested commit order

1. **C** — 2 controllers, one line each (also fixes the latent temporal POST bug).
2. **G** — one helper, error-type only.
3. **F** — grammar validator in `parseProjectionTerm` + the `omitTerm` drive-by + unit tests.

Each is independent; commit separately. After all three, run the full Postman gate on a fresh
aaio stack (the collection requires a fresh stack per run) plus CommonBehaviours (should stay
33/33) as the regression guard.

## Explicitly out of scope here

- **A** (join follow / objectType-vs-local): genuine spec contradiction — see
  `linked-entity-retrieval-objectType-vs-local.md`. No code change recommended.
- **B** (`objectType` echoed): defensible as test overreach; not fixed.
- **D** (500 on `omit` + nested + `join=flat`): a real Scorpio bug (`OmitTerm.java:389` casts a
  String `@type` to `List`) but tracked separately.
- **E, H, I**: separate follow-ups (H needs re-validation; I is likely test overreach per
  4.5.5.1).
