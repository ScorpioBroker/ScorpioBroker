#!/usr/bin/env python3
"""Generate OKF v0.1 NGSI-LD wiki from ETSI GS CIM 009 V1.9.1 text extract."""

from __future__ import annotations

import re
import textwrap
from collections import defaultdict
from pathlib import Path

PDF_URL = (
    "https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/"
    "gs_cim009v010901p.pdf"
)
TIMESTAMP = "2026-07-18T00:00:00Z"
ROOT = Path(__file__).resolve().parents[1]

# Clause prefix -> relative directory under bundle root
DIR_MAP = [
    (re.compile(r"^1$|^2(\.|$)"), "overview"),
    (re.compile(r"^3(\.|$)"), "overview"),
    (re.compile(r"^4\.2(\.|$)"), "framework/information-model"),
    (re.compile(r"^4\.3(\.|$)"), "framework/architecture"),
    (re.compile(r"^4\.4$"), "framework"),
    (re.compile(r"^4\.5(\.|$)"), "framework/data-representation"),
    (re.compile(r"^4\.6(\.|$)"), "framework/restrictions"),
    (re.compile(r"^4\.7(\.|$)|^4\.8$"), "framework/geo-temporal"),
    (re.compile(r"^4\.(9|1[0-9]|2[0-3])(\.|$)"), "framework/languages"),
    (re.compile(r"^4(\.|$)"), "framework"),
    (re.compile(r"^5\.2(\.|$)"), "api-operations/data-types"),
    (re.compile(r"^5\.3(\.|$)"), "api-operations/notifications"),
    (re.compile(r"^5\.4$"), "api-operations"),
    (re.compile(r"^5\.5(\.|$)"), "api-operations/common-behaviours"),
    (re.compile(r"^5\.6(\.|$)"), "api-operations/provision"),
    (re.compile(r"^5\.7(\.|$)"), "api-operations/consumption"),
    (re.compile(r"^5\.8(\.|$)"), "api-operations/subscription"),
    (re.compile(r"^5\.9(\.|$)"), "api-operations/registration"),
    (re.compile(r"^5\.10(\.|$)"), "api-operations/discovery"),
    (re.compile(r"^5\.11(\.|$)"), "api-operations/csource-subscription"),
    (re.compile(r"^5\.12$"), "api-operations/matching"),
    (re.compile(r"^5\.13(\.|$)"), "api-operations/contexts"),
    (re.compile(r"^5\.14(\.|$)"), "api-operations/entity-mapping"),
    (re.compile(r"^5\.15(\.|$)"), "api-operations/source-identity"),
    (re.compile(r"^5\.16(\.|$)"), "api-operations/snapshots"),
    (re.compile(r"^5(\.|$)"), "api-operations"),
    (re.compile(r"^6\.3(\.|$)"), "http-binding/common-behaviours"),
    (re.compile(r"^6(\.|$)"), "http-binding"),
    (re.compile(r"^7(\.|$)"), "mqtt-binding"),
    (re.compile(r"^A(\.|$)"), "annexes"),
    (re.compile(r"^B$"), "annexes"),
    (re.compile(r"^C(\.|$)"), "annexes"),
    (re.compile(r"^D(\.|$)"), "annexes"),
    (re.compile(r"^E$"), "annexes"),
    (re.compile(r"^F$"), "annexes"),
    (re.compile(r"^G(\.|$)"), "annexes"),
    (re.compile(r"^H(\.|$)"), "annexes"),
]

# Operation parents: collapse 5.6.1.1..5.6.1.5 into 5.6.1
OPERATION_PARENT = re.compile(
    r"^(5\.(6|7|8|9|10|11|13|14|15|16)\.\d+)(\.\d+)?$"
)
# HTTP resource parents: collapse 6.4.x into 6.4
HTTP_RESOURCE = re.compile(r"^(6\.(?:[4-9]|[1-3]\d))(\.|$)")

TOC_RE = re.compile(
    r"^((?:\d+(?:\.\d+)*|[A-H](?:\.\d+)*))\s+(.+?)\s+\.\.\.\s+\d+\s*$"
)
BODY_HEADING_RE = re.compile(
    r"^((?:\d+(?:\.\d+)*|[A-H](?:\.\d+)*))\s+(.+?)\s*$"
)
MD_HEADING_RE = re.compile(r"^##\s+((?:\d+(?:\.\d+)*)|[A-H].*?)\s*$")


def slugify(title: str) -> str:
    s = title.lower()
    s = s.replace("@", "at-")
    s = s.replace("/", "-")
    s = s.replace("{", "").replace("}", "")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s[:80] or "section"


def clause_dir(clause: str) -> str:
    for pat, d in DIR_MAP:
        if pat.search(clause):
            return d
    return "overview"


def infer_type(clause: str, title: str) -> str:
    t = title.lower()
    if clause.startswith(("A", "B", "C", "D", "E", "F", "G", "H")):
        return "Reference"
    if re.match(r"^5\.2(\.|$)", clause) or re.match(r"^5\.3(\.|$)", clause):
        return "NGSI-LD Data Type"
    if re.match(r"^5\.(6|7|8|9|10|11|13|14|15|16)\.\d+$", clause):
        return "NGSI-LD Operation"
    if re.match(r"^6\.\d+$", clause) and int(clause.split(".")[1]) >= 4:
        return "NGSI-LD Resource"
    if clause.startswith("3.1") or (clause == "3.1"):
        return "NGSI-LD Term"
    return "NGSI-LD Clause"


def should_emit_standalone(clause: str, title: str) -> bool:
    """Skip ultra-fine operation subparts (Description/Input/…) — fold into parent."""
    # 5.6.1.1 Description -> fold into 5.6.1
    m = re.match(r"^(5\.(6|7|8|9|10|11|13|14|15|16)\.\d+)\.(\d+)$", clause)
    if m:
        return False
    # 6.4.3.1 POST -> fold into 6.4 resource file
    if re.match(r"^6\.\d+\.", clause):
        return False
    # Skip use-case-diagram-only noise titles under ops (already folded)
    return True


def parent_clause(clause: str) -> str | None:
    m = re.match(r"^(5\.(6|7|8|9|10|11|13|14|15|16)\.\d+)\.(\d+)$", clause)
    if m:
        return m.group(1)
    m = re.match(r"^(6\.\d+)\.", clause)
    if m:
        return m.group(1)
    return None


def parse_toc(lines: list[str]) -> list[tuple[str, str]]:
    entries = []
    in_toc = False
    for line in lines:
        if line.strip() == "## Contents":
            in_toc = True
            continue
        if in_toc and line.startswith("## "):
            break
        if not in_toc:
            continue
        m = TOC_RE.match(line.strip())
        if m:
            clause, title = m.group(1), m.group(2).strip()
            # Normalize annex titles
            if clause in "ABCDEFGHI" and "(" in title:
                title = re.sub(r"^\([^)]+\):\s*", "", title)
            entries.append((clause, title))
    return entries


MD_CLAUSE_HEADING = re.compile(
    r"^(#{1,6})\s+((?:\d+(?:\.\d+)*|[A-H](?:\.\d+)*))\s+(.+)$"
)
PLAIN_CLAUSE_HEADING = re.compile(
    r"^((?:\d+(?:\.\d+)+|[A-H](?:\.\d+)*))\s+([A-Za-z@(].*)$"
)


def is_descendant(child: str, parent: str) -> bool:
    return child.startswith(parent + ".")


def find_body_starts(lines: list[str], clauses: set[str]) -> dict[str, int]:
    """Map clause -> best body heading line (prefer markdown headings, last match)."""
    md_starts: dict[str, int] = {}
    plain_starts: dict[str, int] = {}
    content_begin = 1055  # after TOC / front matter

    for i, line in enumerate(lines):
        if i < content_begin:
            continue
        if "..." in line:
            continue
        s = line.strip()
        m = MD_CLAUSE_HEADING.match(s)
        if m and m.group(2) in clauses:
            md_starts[m.group(2)] = i
            continue
        m = PLAIN_CLAUSE_HEADING.match(s)
        if m and m.group(1) in clauses:
            # Skip list-like short titles without real section follow-through:
            # keep last plain match only if no md heading exists later
            plain_starts[m.group(1)] = i

    starts = dict(plain_starts)
    starts.update(md_starts)  # markdown wins
    return starts


def clean_text(text: str) -> str:
    # Fix common PDF extract artifacts
    text = text.replace("\uf0a7", "-")
    text = text.replace("\u2022", "-")
    text = text.replace("�", "-")
    text = text.replace("\ufffd", "-")
    text = re.sub(r"[ \t]+\n", "\n", text)
    return text


def extract_section(
    lines: list[str],
    clause: str,
    starts: dict[str, int],
    ordered: list[str],
) -> str:
    if clause not in starts:
        return ""
    start = starts[clause]
    end = len(lines)
    # End at next non-descendant clause that has a start after ours
    candidates = []
    for nxt in ordered:
        if nxt == clause:
            continue
        if nxt not in starts:
            continue
        if starts[nxt] <= start:
            continue
        if is_descendant(nxt, clause):
            continue
        candidates.append(starts[nxt])
    if candidates:
        end = min(candidates)

    chunk = lines[start:end]
    if chunk:
        chunk = chunk[1:]
    out = []
    blank = 0
    for ln in chunk:
        s = ln.strip()
        if s == "ETSI":
            continue
        # Strip leading markdown hashes from internal headings for cleaner body
        ln2 = re.sub(r"^#{1,6}\s+", "", ln.rstrip())
        if not ln2.strip():
            blank += 1
            if blank > 2:
                continue
        else:
            blank = 0
        out.append(ln2)
    text = clean_text("\n".join(out).strip())
    if len(text) > 25000:
        text = text[:25000] + "\n\n… (truncated; see full clause in the PDF)."
    return text


def yaml_escape(s: str) -> str:
    s = s.replace('"', '\\"')
    return s


def write_concept(
    path: Path,
    *,
    type_: str,
    title: str,
    description: str,
    clause: str,
    body: str,
    tags: list[str],
    related: list[tuple[str, str]] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tags_yaml = "[" + ", ".join(tags) + "]"
    desc = description.replace("\n", " ").strip()
    if len(desc) > 220:
        desc = desc[:217] + "…"
    parts = [
        "---",
        f'type: {type_}',
        f'title: "{yaml_escape(title)}"',
        f'description: "{yaml_escape(desc)}"',
        f"resource: {PDF_URL}#clause-{clause}",
        f"tags: {tags_yaml}",
        f"timestamp: {TIMESTAMP}",
        f'clause: "{clause}"',
        "---",
        "",
    ]
    if body:
        parts.append(body)
        parts.append("")
    else:
        parts.append(
            f"Normative content for clause **{clause}** ({title}). "
            "See the cited PDF section for the authoritative definition."
        )
        parts.append("")
    if related:
        parts.append("# Related")
        parts.append("")
        for label, link in related:
            parts.append(f"* [{label}]({link})")
        parts.append("")
    parts.append("# Citations")
    parts.append("")
    parts.append(
        f"[1] [ETSI GS CIM 009 V1.9.1 clause {clause}]({PDF_URL}) — {title}"
    )
    parts.append("")
    path.write_text("\n".join(parts), encoding="utf-8")


def write_index(dir_path: Path, title: str, entries: list[tuple[str, str, str]]) -> None:
    """entries: (display title, relative url, description)"""
    dir_path.mkdir(parents=True, exist_ok=True)
    lines = [f"# {title}", ""]
    # Group by first path segment if mixed — keep flat section
    lines.append(f"# {title}")
    lines.append("")
    for t, url, desc in entries:
        d = desc.strip() if desc else ""
        if d:
            lines.append(f"* [{t}]({url}) - {d}")
        else:
            lines.append(f"* [{t}]({url})")
    lines.append("")
    (dir_path / "index.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    extract = Path(
        r"C:\Users\hebgen\.cursor\projects\c-Users-hebgen-scorpiodev-master-scorpiobroker"
        r"\agent-tools\27de9d62-67cc-43b6-9300-bc14d7f76bbd.txt"
    )
    if not extract.exists():
        raise SystemExit(f"Extract not found: {extract}")

    raw = extract.read_text(encoding="utf-8", errors="replace")
    lines = raw.splitlines()

    toc = parse_toc(lines)
    print(f"TOC entries: {len(toc)}")

    # Build emit list: standalone clauses + folded operation/resource parents
    by_clause = {c: t for c, t in toc}
    emit: dict[str, str] = {}
    folded_children: dict[str, list[str]] = defaultdict(list)

    for clause, title in toc:
        if should_emit_standalone(clause, title):
            emit[clause] = title
        else:
            p = parent_clause(clause)
            if p:
                folded_children[p].append(clause)
                if p not in emit and p in by_clause:
                    emit[p] = by_clause[p]
            # ensure parent exists even if only children in toc
            if p and p not in emit:
                # synthesize from parent numbering
                emit[p] = by_clause.get(p, title)

    # Ensure parents from TOC that are operations are present
    for clause, title in toc:
        if re.match(r"^5\.(6|7|8|9|10|11|13|14|15|16)\.\d+$", clause):
            emit[clause] = title
        if re.match(r"^6\.\d+$", clause) and int(clause.split(".")[1]) >= 4:
            emit[clause] = title

    ordered_all = [c for c, _ in toc]
    # Also include emit-only parents
    for c in emit:
        if c not in ordered_all:
            ordered_all.append(c)

    starts = find_body_starts(lines, set(ordered_all) | set(emit))
    print(f"Body starts found: {len(starts)}")

    # For folded ops, concatenate child section bodies
    def body_for(clause: str) -> str:
        kids = folded_children.get(clause, [])
        if kids:
            chunks = []
            for k in kids:
                title = by_clause.get(k, k)
                # subheading from last segment title
                sub = by_clause.get(k, "")
                # Use Description/Input style from TOC title
                heading = sub.split()[-1] if False else sub
                # Prefer last part after clause in title — title IS the subsection name
                h = by_clause.get(k, k)
                # strip leading clause if present
                h = re.sub(r"^" + re.escape(k) + r"\s*", "", h)
                content = extract_section(lines, k, starts, ordered_all)
                if not content:
                    content = extract_section(lines, clause, starts, ordered_all)
                chunks.append(f"## {h}\n\n{content}" if content else f"## {h}\n")
            # Also prepend parent-level intro if any
            parent_body = extract_section(lines, clause, starts, ordered_all)
            # Parent body often includes all children already — if long, use parent only
            if parent_body and len(parent_body) > 400:
                return parent_body
            return "\n\n".join(chunks).strip()
        return extract_section(lines, clause, starts, ordered_all)

    # File path map
    file_paths: dict[str, Path] = {}
    index_entries: dict[str, list[tuple[str, str, str]]] = defaultdict(list)

    for clause, title in sorted(emit.items(), key=lambda x: natural_key(x[0])):
        d = clause_dir(clause)
        fname = f"{slugify(title)}.md"
        # Disambiguate collisions
        rel = Path(d) / fname
        abs_path = ROOT / rel
        if abs_path in file_paths.values() or abs_path.exists():
            fname = f"{slugify(title)}-{clause.replace('.', '-')}.md"
            rel = Path(d) / fname
            abs_path = ROOT / rel
        file_paths[clause] = abs_path

        body = body_for(clause)
        desc = first_sentence(body) or f"NGSI-LD specification clause {clause}: {title}."
        type_ = infer_type(clause, title)
        tags = ["ngsi-ld", "cim009", "v1.9.1"]
        if d.startswith("api-operations"):
            tags.append("api")
        if d.startswith("http-binding"):
            tags.append("http")
        if d.startswith("framework"):
            tags.append("framework")

        write_concept(
            abs_path,
            type_=type_,
            title=title,
            description=desc,
            clause=clause,
            body=body if body else f"See clause {clause} in ETSI GS CIM 009 V1.9.1.",
            tags=tags,
        )
        # Index entry relative to directory
        index_entries[d].append((title, fname, desc[:120]))

    print(f"Concepts written: {len(file_paths)}")

    # Directory indexes
    dir_titles = {
        "overview": "Overview (clauses 1–3)",
        "framework": "Context Information Management Framework (clause 4)",
        "framework/information-model": "NGSI-LD Information Model",
        "framework/architecture": "Architectural Considerations",
        "framework/data-representation": "Data Representation",
        "framework/restrictions": "Data Representation Restrictions",
        "framework/geo-temporal": "Geospatial and Temporal Properties",
        "framework/languages": "Query Languages and Related Features",
        "api-operations": "API Operation Definition (clause 5)",
        "api-operations/data-types": "Data Types",
        "api-operations/notifications": "Notification Data Types",
        "api-operations/common-behaviours": "Common Behaviours",
        "api-operations/provision": "Context Information Provision",
        "api-operations/consumption": "Context Information Consumption",
        "api-operations/subscription": "Context Information Subscription",
        "api-operations/registration": "Context Source Registration",
        "api-operations/discovery": "Context Source Discovery",
        "api-operations/csource-subscription": "Context Source Registration Subscription",
        "api-operations/matching": "Matching Context Source Registrations",
        "api-operations/contexts": "Storing, Managing and Serving @contexts",
        "api-operations/entity-mapping": "Context Source Entity Mapping",
        "api-operations/source-identity": "Context Source Identity Information",
        "api-operations/snapshots": "Snapshot Functionality",
        "http-binding": "API HTTP Binding (clause 6)",
        "http-binding/common-behaviours": "HTTP Common Behaviours",
        "mqtt-binding": "API MQTT Notification Binding (clause 7)",
        "annexes": "Annexes",
    }

    # Nested framework index lists subdirs
    for d, entries in index_entries.items():
        title = dir_titles.get(d, d)
        write_index(ROOT / d, title, entries)

    # Parent indexes that only have subdirs
    write_framework_parent_indexes(index_entries, dir_titles)
    write_api_parent_indexes(index_entries, dir_titles)
    write_http_parent_index(index_entries, dir_titles)

    # Root index with OKF version frontmatter (only place allowed)
    root_index = textwrap.dedent(
        f"""\
        ---
        okf_version: "0.1"
        ---

        # NGSI-LD Knowledge Bundle (ETSI GS CIM 009 V1.9.1)

        OKF v0.1 agent-readable wiki of the NGSI-LD API specification.
        Paraphrased/structured for progressive disclosure — cite the PDF for normative authority.

        # Sections

        * [Overview](overview/) - Scope, references, terms, symbols, and abbreviations (clauses 1–3)
        * [Framework](framework/) - Information model, architecture, representations, languages (clause 4)
        * [API Operations](api-operations/) - Data types, behaviours, and operations (clause 5)
        * [HTTP Binding](http-binding/) - REST resources and HTTP behaviours (clause 6)
        * [MQTT Binding](mqtt-binding/) - MQTT notification binding (clause 7)
        * [Annexes](annexes/) - Identifiers, core @context, examples, algorithms, localization

        # Spec

        * [ETSI GS CIM 009 V1.9.1 (PDF)]({PDF_URL})
        * [OKF SPEC](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md)
        """
    )
    (ROOT / "index.md").write_text(root_index, encoding="utf-8")

    log = textwrap.dedent(
        """\
        # Directory Update Log

        ## 2026-07-18
        * **Initialization**: Created maximal OKF v0.1 NGSI-LD knowledge bundle from ETSI GS CIM 009 V1.9.1.
        * **Creation**: Populated overview, framework, api-operations, http-binding, mqtt-binding, and annexes.
        """
    )
    (ROOT / "log.md").write_text(log, encoding="utf-8")
    print("Done.")


def write_framework_parent_indexes(index_entries, dir_titles):
    entries = [
        ("Information Model", "information-model/", "Clause 4.2 meta-model and ontology"),
        ("Architecture", "architecture/", "Clause 4.3 centralized/distributed/federated"),
        ("Data Representation", "data-representation/", "Clause 4.5 representations"),
        ("Restrictions", "restrictions/", "Clause 4.6"),
        ("Geo / Temporal Properties", "geo-temporal/", "Clauses 4.7–4.8"),
        ("Languages & features", "languages/", "Clauses 4.9–4.23"),
    ]
    local = index_entries.get("framework", [])
    lines = ["# Context Information Management Framework (clause 4)", "", "# Sections", ""]
    for e in entries:
        lines.append(f"* [{e[0]}]({e[1]}) - {e[2]}")
    for t, url, desc in local:
        lines.append(f"* [{t}]({url}) - {desc}")
    lines.append("")
    (ROOT / "framework" / "index.md").write_text("\n".join(lines), encoding="utf-8")


def write_api_parent_indexes(index_entries, dir_titles):
    subs = [
        ("Data Types", "data-types/", "Clause 5.2"),
        ("Notification data types", "notifications/", "Clause 5.3"),
        ("Common behaviours", "common-behaviours/", "Clause 5.5"),
        ("Provision", "provision/", "Clause 5.6"),
        ("Consumption", "consumption/", "Clause 5.7"),
        ("Subscription", "subscription/", "Clause 5.8"),
        ("Registration", "registration/", "Clause 5.9"),
        ("Discovery", "discovery/", "Clause 5.10"),
        ("CSource subscription", "csource-subscription/", "Clause 5.11"),
        ("Matching registrations", "matching/", "Clause 5.12"),
        ("@contexts", "contexts/", "Clause 5.13"),
        ("Entity mapping", "entity-mapping/", "Clause 5.14"),
        ("Source identity", "source-identity/", "Clause 5.15"),
        ("Snapshots", "snapshots/", "Clause 5.16"),
    ]
    lines = ["# API Operation Definition (clause 5)", "", "# Sections", ""]
    for t, u, d in subs:
        lines.append(f"* [{t}]({u}) - {d}")
    for t, url, desc in index_entries.get("api-operations", []):
        lines.append(f"* [{t}]({url}) - {desc}")
    lines.append("")
    (ROOT / "api-operations" / "index.md").write_text("\n".join(lines), encoding="utf-8")


def write_http_parent_index(index_entries, dir_titles):
    lines = ["# API HTTP Binding (clause 6)", "", "# Sections", ""]
    lines.append(
        "* [Common behaviours](common-behaviours/) - Clause 6.3 HTTP binding behaviours"
    )
    for t, url, desc in index_entries.get("http-binding", []):
        lines.append(f"* [{t}]({url}) - {desc}")
    lines.append("")
    (ROOT / "http-binding" / "index.md").write_text("\n".join(lines), encoding="utf-8")


def first_sentence(text: str) -> str:
    t = re.sub(r"^#+\s+.*$", "", text, flags=re.M).strip()
    t = re.sub(r"\s+", " ", t)
    if not t:
        return ""
    m = re.match(r"(.{20,200}?[.!?])\s", t + " ")
    return m.group(1) if m else t[:200]


def natural_key(clause: str):
    parts = []
    for p in re.split(r"\.", clause):
        if p.isdigit():
            parts.append((0, int(p)))
        else:
            parts.append((1, p))
    return parts


if __name__ == "__main__":
    main()
