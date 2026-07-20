#!/usr/bin/env python3
"""Add Related cross-links between OKF concepts (ops <-> HTTP resources, clause refs)."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Operation clause -> HTTP resource concept path (bundle-absolute)
OP_TO_HTTP = {
    "5.6.1": "/http-binding/resource-entities.md",
    "5.6.2": "/http-binding/resource-entities-entityid-attrs.md",
    "5.6.3": "/http-binding/resource-entities-entityid-attrs.md",
    "5.6.4": "/http-binding/resource-entities-entityid-attrs-attrid.md",
    "5.6.5": "/http-binding/resource-entities-entityid-attrs-attrid.md",
    "5.6.6": "/http-binding/resource-entities-entityid.md",
    "5.6.7": "/http-binding/resource-entityoperations-create.md",
    "5.6.8": "/http-binding/resource-entityoperations-upsert.md",
    "5.6.9": "/http-binding/resource-entityoperations-update.md",
    "5.6.10": "/http-binding/resource-entityoperations-delete.md",
    "5.6.11": "/http-binding/resource-temporal-entities.md",
    "5.6.12": "/http-binding/resource-temporal-entities-entityid-attrs.md",
    "5.6.13": "/http-binding/resource-temporal-entities-entityid-attrs-attrid.md",
    "5.6.14": "/http-binding/resource-temporal-entities-entityid-attrs-attrid-instanceid.md",
    "5.6.15": "/http-binding/resource-temporal-entities-entityid-attrs-attrid-instanceid.md",
    "5.6.16": "/http-binding/resource-temporal-entities-entityid.md",
    "5.6.17": "/http-binding/resource-entities-entityid.md",
    "5.6.18": "/http-binding/resource-entities-entityid.md",
    "5.6.19": "/http-binding/resource-entities-entityid-attrs-attrid.md",
    "5.6.20": "/http-binding/resource-entityoperations-merge.md",
    "5.6.21": "/http-binding/resource-entities.md",
    "5.7.1": "/http-binding/resource-entities-entityid.md",
    "5.7.2": "/http-binding/resource-entities.md",
    "5.7.3": "/http-binding/resource-temporal-entities-entityid.md",
    "5.7.4": "/http-binding/resource-temporal-entities.md",
    "5.7.5": "/http-binding/resource-types.md",
    "5.7.6": "/http-binding/resource-types.md",
    "5.7.7": "/http-binding/resource-types-type.md",
    "5.7.8": "/http-binding/resource-attributes.md",
    "5.7.9": "/http-binding/resource-attributes.md",
    "5.7.10": "/http-binding/resource-attributes-attrid.md",
    "5.8.1": "/http-binding/resource-subscriptions.md",
    "5.8.2": "/http-binding/resource-subscriptions-subscriptionid.md",
    "5.8.3": "/http-binding/resource-subscriptions-subscriptionid.md",
    "5.8.4": "/http-binding/resource-subscriptions.md",
    "5.8.5": "/http-binding/resource-subscriptions-subscriptionid.md",
    "5.9.2": "/http-binding/resource-csourceregistrations.md",
    "5.9.3": "/http-binding/resource-csourceregistrations-registrationid.md",
    "5.9.4": "/http-binding/resource-csourceregistrations-registrationid.md",
    "5.10.1": "/http-binding/resource-csourceregistrations-registrationid.md",
    "5.10.2": "/http-binding/resource-csourceregistrations.md",
    "5.11.2": "/http-binding/resource-csourcesubscriptions.md",
    "5.11.3": "/http-binding/resource-csourcesubscriptions-subscriptionid.md",
    "5.11.4": "/http-binding/resource-csourcesubscriptions-subscriptionid.md",
    "5.11.5": "/http-binding/resource-csourcesubscriptions.md",
    "5.11.6": "/http-binding/resource-csourcesubscriptions-subscriptionid.md",
    "5.13.2": "/http-binding/resource-jsonldcontexts.md",
    "5.13.3": "/http-binding/resource-jsonldcontexts.md",
    "5.13.4": "/http-binding/resource-jsonldcontexts-contextid.md",
    "5.13.5": "/http-binding/resource-jsonldcontexts-contextid.md",
    "5.14.1": "/http-binding/resource-entitymaps-entitymapid.md",
    "5.14.2": "/http-binding/resource-entitymaps-entitymapid.md",
    "5.14.3": "/http-binding/resource-entitymaps-entitymapid.md",
    "5.14.4": "/http-binding/resource-entitymaps.md",
    "5.14.5": "/http-binding/resource-temporal-entitymaps.md",
    "5.15.1": "/http-binding/resource-info-sourceidentity.md",
    "5.16.1": "/http-binding/resource-snapshots.md",
    "5.16.2": "/http-binding/resource-snapshots-snapshotid-clone.md",
    "5.16.3": "/http-binding/resource-snapshots-snapshotid.md",
    "5.16.4": "/http-binding/resource-snapshots-snapshotid.md",
    "5.16.5": "/http-binding/resource-snapshots-snapshotid.md",
    "5.16.7": "/http-binding/resource-snapshots.md",
}

# Framework cross-links for key representation types
EXTRA_RELATED = {
    "4.5.2.2": [
        ("Concise Property", "/framework/data-representation/concise-ngsi-ld-property.md"),
        ("Property data type", "/api-operations/data-types/property.md"),
    ],
    "4.5.3.2": [
        ("Concise Relationship", "/framework/data-representation/concise-ngsi-ld-relationship.md"),
        ("Relationship data type", "/api-operations/data-types/relationship.md"),
    ],
    "4.9": [
        ("Query Entities", "/api-operations/consumption/query-entities.md"),
        ("Geoquery Language", "/framework/languages/ngsi-ld-geoquery-language.md"),
    ],
    "5.5.12": [
        ("Merge Entity", "/api-operations/provision/merge-entity.md"),
        ("Partial Update Patch", "/api-operations/common-behaviours/partial-update-patch-behaviour.md"),
    ],
}


def load_clause_index() -> dict[str, str]:
    """clause -> /bundle/path.md"""
    idx = {}
    for f in ROOT.rglob("*.md"):
        if f.name in ("index.md", "log.md"):
            continue
        text = f.read_text(encoding="utf-8")
        m = re.search(r'^clause:\s*"([^"]+)"', text, re.M)
        if not m:
            continue
        rel = "/" + f.relative_to(ROOT).as_posix()
        idx[m.group(1)] = rel
    return idx


def ensure_related(text: str, links: list[tuple[str, str]]) -> str:
    if not links:
        return text
    # Remove existing Related section if regenerating
    text = re.sub(
        r"\n# Related\n.*?(?=\n# Citations\n)",
        "\n",
        text,
        flags=re.S,
    )
    block = ["# Related", ""]
    seen = set()
    for label, url in links:
        key = (label, url)
        if key in seen:
            continue
        seen.add(key)
        # Only keep if target exists
        target = ROOT / url.lstrip("/")
        if not target.exists():
            continue
        block.append(f"* [{label}]({url})")
    if len(block) == 2:
        return text
    block.append("")
    if "# Citations" in text:
        return text.replace("# Citations", "\n".join(block) + "# Citations", 1)
    return text.rstrip() + "\n\n" + "\n".join(block) + "\n"


def title_from_file(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    m = re.search(r'^title:\s*"([^"]+)"', text, re.M)
    return m.group(1) if m else path.stem


def main() -> None:
    clause_idx = load_clause_index()
    print(f"Indexed {len(clause_idx)} concepts")

    # Invert HTTP -> ops
    http_to_ops: dict[str, list[str]] = {}
    for op, http in OP_TO_HTTP.items():
        http_to_ops.setdefault(http, []).append(op)

    updated = 0
    for f in ROOT.rglob("*.md"):
        if f.name in ("index.md", "log.md"):
            continue
        text = f.read_text(encoding="utf-8")
        m = re.search(r'^clause:\s*"([^"]+)"', text, re.M)
        if not m:
            continue
        clause = m.group(1)
        links: list[tuple[str, str]] = []

        if clause in OP_TO_HTTP:
            http = OP_TO_HTTP[clause]
            hp = ROOT / http.lstrip("/")
            if hp.exists():
                links.append((f"HTTP: {title_from_file(hp)}", http))

        rel_path = "/" + f.relative_to(ROOT).as_posix()
        if rel_path in http_to_ops:
            for op in http_to_ops[rel_path]:
                if op in clause_idx:
                    op_path = clause_idx[op]
                    links.append((f"Operation: {title_from_file(ROOT / op_path.lstrip('/'))}", op_path))

        if clause in EXTRA_RELATED:
            links.extend(EXTRA_RELATED[clause])

        # Discover clause X.Y refs in body and link a few
        body = text.split("---", 2)[-1] if text.count("---") >= 2 else text
        refs = set(re.findall(r"clause\s+(\d+(?:\.\d+){1,4})", body, flags=re.I))
        for ref in sorted(refs)[:8]:
            if ref == clause:
                continue
            if ref in clause_idx:
                links.append((f"Clause {ref}", clause_idx[ref]))

        new_text = ensure_related(text, links)
        if new_text != text:
            f.write_text(new_text, encoding="utf-8")
            updated += 1

    print(f"Updated Related on {updated} files")


if __name__ == "__main__":
    main()
