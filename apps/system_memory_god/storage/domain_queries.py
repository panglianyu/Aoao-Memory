"""Bounded domain projections over one scoped, caller-owned SQLite snapshot."""

from datetime import datetime
import json
import time

from kernel.daily_memory_contracts import memory_query_terms as query_terms

DOMAINS = ("vector", "entity", "graph", "rule", "journal", "schedule")


def _has_uncovered_entity_versions(db, pairs, covered, deadline) -> bool:
    # Omitted pairs are already bounded by the initial candidate limit. Prove
    # coverage in this same snapshot, without lexical filtering or expansion.
    if deadline is not None and time.monotonic() >= deadline:
        raise ValueError("domain query deadline exceeded")
    match = " OR ".join("(json_extract(payload_json,'$.subject')=? AND "
                           "json_extract(payload_json,'$.attribute')=?)" for _ in pairs)
    marks = ",".join("?" for _ in covered)
    row = db.execute("SELECT 1 FROM derived_evidence WHERE domain='entity' AND ("
        + match + ") AND memory_id NOT IN (" + marks + ") LIMIT 1",
        (*[value for pair in pairs for value in pair], *covered)).fetchone()
    if deadline is not None and time.monotonic() >= deadline:
        raise ValueError("domain query deadline exceeded")
    return row is not None


def query_domain_ids(db, query: str, *, domains: tuple[str, ...], limit=100,
                     deadline=None, event_start_ts=None, event_end_ts=None, timings=None) -> tuple[str, ...]:
    if (type(domains) is not tuple or not set(domains).issubset(DOMAINS)
            or type(limit) is not int or not 1 <= limit <= 100):
        raise ValueError("invalid domain selection")
    terms = query_terms(query)
    if deadline is not None:
        if time.monotonic() >= deadline:
            raise ValueError("domain query deadline exceeded")
        db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 250)
    try:
        per_domain = []
        for domain in DOMAINS:
            if domain not in domains:
                continue
            began = time.monotonic()
            # Payload JSON uses canonical ASCII escapes; search_text contains the
            # actual original language. Bound scans via SQLite progress handler.
            match = " OR ".join("instr(lower(e.search_text),?)>0" for _ in terms)
            rows = db.execute("SELECT d.memory_id,d.payload_json FROM derived_evidence d "
                "JOIN evidence_search e ON d.memory_id=e.memory_id WHERE d.domain=? AND ("
                + match + ") ORDER BY d.memory_id LIMIT ?", (domain, *terms, limit + 1)).fetchall()
            if len(rows) > limit and timings is not None:
                timings[domain + "_limited"] = True
            rows = rows[:limit]
            selected = []
            for memory_id, encoded in rows:
                payload = json.loads(encoded)
                if domain in {"journal", "schedule"} and (event_start_ts is not None or event_end_ts is not None):
                    stamp = payload.get("occurred_at" if domain == "journal" else "scheduled_at")
                    if stamp is None:
                        continue
                    value = datetime.fromisoformat(stamp).timestamp()
                    if (event_start_ts is not None and value < event_start_ts
                            or event_end_ts is not None and value >= event_end_ts):
                        continue
                selected.append((memory_id, payload))
            if domain == "entity":
                pairs = tuple(dict.fromkeys((p["subject"], p["attribute"]) for _, p in selected))
                if len(pairs) > 5:
                    uncovered = _has_uncovered_entity_versions(
                        db, pairs[5:], tuple(key for key, _ in selected), deadline)
                    if uncovered and timings is not None:
                        timings[domain + "_limited"] = True
                for subject, attribute in pairs[:5]:
                    versions = db.execute("SELECT memory_id,payload_json FROM derived_evidence "
                        "WHERE domain='entity' AND json_extract(payload_json,'$.subject')=? "
                        "AND json_extract(payload_json,'$.attribute')=? ORDER BY memory_id LIMIT ?",
                        (subject, attribute, limit + 1)).fetchall()
                    if len(versions) > limit and timings is not None:
                        timings[domain + "_limited"] = True
                    selected.extend((key, json.loads(body)) for key, body in versions[:limit])
            if domain == "graph":
                direct = [(key, p) for key, p in selected if any(
                    p[field].lower() in query.lower() for field in ("subject", "object"))]
                # Seeds are fixed before expansion. Newly found endpoints never
                # become seeds: exactly one hop, five seeds, eight edges each.
                possible_seeds = tuple(dict.fromkeys(p[field] for _, p in direct or selected
                    for field in ("subject", "object")))
                seeds = possible_seeds[:5]
                counts = dict.fromkeys(seeds, 0)
                seen: set[str] = set()
                initial, selected = selected, []
                if len(possible_seeds) > 5 and timings is not None:
                    timings[domain + "_limited"] = True

                def admit(key, payload):
                    if key in seen:
                        return
                    owners = [seed for seed in seeds if seed in (payload["subject"], payload["object"])]
                    if not owners or any(counts[seed] >= 8 for seed in owners):
                        if timings is not None:
                            timings[domain + "_limited"] = True
                        return
                    seen.add(key)
                    selected.append((key, payload))
                    for seed in owners:
                        counts[seed] += 1

                for key, payload in initial:
                    admit(key, payload)
                for seed in seeds:
                    rows = db.execute("SELECT memory_id,payload_json FROM derived_evidence "
                        "WHERE domain='graph' AND (json_extract(payload_json,'$.subject')=? "
                        "OR json_extract(payload_json,'$.object')=?) ORDER BY memory_id LIMIT 9",
                        (seed, seed)).fetchall()
                    if len(rows) > 8 and timings is not None:
                        timings[domain + "_limited"] = True
                    for key, body in rows[:8]:
                        admit(key, json.loads(body))
            per_domain.append(tuple(dict.fromkeys(key for key, _ in selected)))
            if timings is not None:
                timings[domain] = (time.monotonic() - began) * 1000
        merged = []
        for offset in range(max((len(items) for items in per_domain), default=0)):
            for items in per_domain:
                if offset < len(items) and items[offset] not in merged:
                    merged.append(items[offset])
                    if len(merged) == limit:
                        if sum(len(items) for items in per_domain) > limit and timings is not None:
                            timings["combined_limited"] = True
                        return tuple(merged)
        return tuple(merged)
    finally:
        if deadline is not None:
            db.set_progress_handler(None, 0)
