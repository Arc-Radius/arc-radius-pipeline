from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv

from arc_pipeline.core.transform import active_sessions, extract_sessions
from arc_pipeline.resources.legiscan_resource import LegiScanClient

PREVIEW_LIMIT = 25


def main() -> None:
    repo_root = Path(__file__).resolve().parent
    load_dotenv(repo_root / ".env")

    client = LegiScanClient()
    payload = client.get_session_list()

    sessions = extract_sessions(payload)
    active = active_sessions(sessions)

    print(f"total_sessions={len(sessions)}")
    print(f"active_sessions={len(active)}")

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
