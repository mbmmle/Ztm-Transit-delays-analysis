import argparse
import psycopg2

from config import DB_CONFIG


def main():
    parser = argparse.ArgumentParser(
        description="Quick cleanup of contaminated terminal-loop rows in gold.bus_delays"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply DELETE. Without this flag, script only prints candidate count.",
    )
    args = parser.parse_args()

    where_clause = """
        stop_sequence <= 1
        AND (
            delay_minutes > 10
            OR distance > 3000
        )
    """

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        cur.execute("SELECT COUNT(*) FROM gold.bus_delays;")
        before_total = cur.fetchone()[0]

        cur.execute(f"SELECT COUNT(*) FROM gold.bus_delays WHERE {where_clause};")
        to_delete = cur.fetchone()[0]

        print(f"[Cleanup_Gold_Bus_Delays] Total rows before: {before_total}")
        print(f"[Cleanup_Gold_Bus_Delays] Candidate contaminated rows: {to_delete}")

        if not args.execute:
            print("[Cleanup_Gold_Bus_Delays] Dry-run mode (no rows deleted). Use --execute to apply.")
            conn.rollback()
            return

        cur.execute(f"DELETE FROM gold.bus_delays WHERE {where_clause};")
        deleted = cur.rowcount

        cur.execute("SELECT COUNT(*) FROM gold.bus_delays;")
        after_total = cur.fetchone()[0]

        conn.commit()

        print(f"[Cleanup_Gold_Bus_Delays] Deleted rows: {deleted}")
        print(f"[Cleanup_Gold_Bus_Delays] Total rows after: {after_total}")

    except Exception as exc:
        conn.rollback()
        print(f"[Cleanup_Gold_Bus_Delays] ERROR: {exc}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
