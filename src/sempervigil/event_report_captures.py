"""One complete publisher capture per normalized URL; private reversible history."""
from datetime import datetime, timezone
from .enrichment.url import normalize_url

WORKFLOW = 'normalized-publisher-captures-v1'


def select(rows, membership):
    groups = {}
    for row in rows: groups.setdefault(normalize_url(row[2]), []).append(row)
    members = {m['article_id']: m for m in membership}
    selected, aliases, history = [], {}, []
    def rank(row):
        captured = row[7] if len(row) > 7 else row[5]
        try:
            dt = datetime.fromisoformat(str(captured).replace('Z', '+00:00'))
            stamp = (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()
        except (ValueError, TypeError): stamp = float('-inf')
        return stamp, row[0]
    for url, versions in sorted(groups.items()):
        usable = [r for r in versions if len(r) < 10 or
                  (r[8] in (None, '1', 'true') and not r[9])]
        if not usable:
            raise ValueError('event_source_report_full_capture_unavailable')
        chosen = max(usable, key=rank)
        selected.append(chosen)
        aliases[chosen[0]] = [members[r[0]] for r in versions if r[0] != chosen[0]]
        if len(versions) > 1:
            history.append({'normalized_url': url, 'selected_article_id': chosen[0],
                            'captures': [{**members[r[0]], 'text': r[3],
                                'captured_at': r[7] if len(r) > 7 else r[5]} for r in versions]})
    return sorted(selected, key=lambda r: r[0]), aliases, history
