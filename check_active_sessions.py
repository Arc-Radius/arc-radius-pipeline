from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv

from arc_pipeline.core.transform import active_sessions, as_int, extract_sessions
from arc_pipeline.resources.legiscan_resource import LegiScanClient

PREVIEW_LIMIT = 25


def main() -> None:
    repo_root = Path(__file__).resolve().parent
    load_dotenv(repo_root / ".env")

    client = LegiScanClient()
    payload = client.get_session_list()
    dataset_list_payload = client.get_dataset_list()

    sessions = extract_sessions(payload)
    active = active_sessions(sessions)
    active_ids = {
        as_int(session.get("session_id"))
        for session in active
        if as_int(session.get("session_id")) > 0
    }

    downloadable_ids = {
        as_int(row.get("session_id"))
        for row in dataset_list_payload.get("datasetlist", [])
        if isinstance(row, dict) and str(row.get("access_key", "")).strip() and as_int(row.get("session_id")) > 0
    }
    n = len(active_ids & downloadable_ids)

    print(f"total_sessions={len(sessions)}")
    print(f"active_sessions={len(active)}")
    print(f"N={n}")
    print(f"projected_api_calls_first_run={2 + n}")

    state_counts = Counter(
        str(session.get("state", "")).upper()
        for session in active
        if str(session.get("state", "")).strip()
    )
    print("active_by_state:")
    for state, count in sorted(state_counts.items()):
        print(f"  {state}: {count}")

    preview = [
        {
            "session_id": session.get("session_id"),
            "state": session.get("state"),
            "year_start": session.get("year_start"),
            "year_end": session.get("year_end"),
            "name": session.get("name"),
        }
        for session in active[:PREVIEW_LIMIT]
    ]
    print("\nfirst_active_sessions:")
    print(json.dumps(preview, indent=2))


if __name__ == "__main__":
    main()
