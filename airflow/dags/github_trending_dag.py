"""
GitHub Trending Crawler DAG

Crawls https://github.com/trending daily and inserts results
into gahyeonbot's PostgreSQL github_trending table.
"""

import os
import re
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

default_args = {
    "owner": "crawler-system",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

GITHUB_TRENDING_URL = "https://github.com/trending"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


def _get_pg_conn():
    return psycopg2.connect(
        host=os.environ.get("GAHYEONBOT_DB_HOST", "host.docker.internal"),
        port=int(os.environ.get("GAHYEONBOT_DB_PORT", "15432")),
        user=os.environ.get("GAHYEONBOT_DB_USER", "gahyeonbot_app"),
        password=os.environ.get("GAHYEONBOT_DB_PASSWORD", ""),
        dbname=os.environ.get("GAHYEONBOT_DB_NAME", "gahyeonbot"),
    )

def _parse_count(text: str) -> int:
    """
    Parse GitHub count strings like "12,345", "1.2k", "3M" into an int.

    GitHub UI sometimes uses k/M abbreviations on compact layouts.
    """
    if not text:
        return 0
    s = text.strip().lower().replace(",", "")
    m = re.search(r"([\d]+(?:\.[\d]+)?)\s*([km]?)", s)
    if not m:
        return 0
    num = float(m.group(1))
    unit = m.group(2)
    if unit == "k":
        num *= 1_000
    elif unit == "m":
        num *= 1_000_000
    return int(num)


def _ensure_events_table(conn) -> None:
    """
    Table for "newly added since last snapshot" events.

    봇은 이 테이블에서 sent_at IS NULL 같은 조건으로 폴링하면 됨.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS github_trending_events (
        id BIGSERIAL PRIMARY KEY,
        snapshot_date DATE NOT NULL,
        repo_full_name TEXT NOT NULL,
        repo_url TEXT NOT NULL,
        description TEXT NULL,
        language TEXT NULL,
        stars_total INTEGER NOT NULL DEFAULT 0,
        stars_period INTEGER NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        sent_at TIMESTAMPTZ NULL
    );
    """
    ddl_idx = """
    CREATE UNIQUE INDEX IF NOT EXISTS github_trending_events_uq
        ON github_trending_events (snapshot_date, repo_full_name);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
        cur.execute(ddl_idx)
    conn.commit()

def preflight_gahyeonbot_schema(**context):
    """
    Fail fast if the required snapshot table doesn't exist.

    We intentionally do NOT auto-create github_trending here: schema should be
    managed by the application migration tool (e.g., Flyway) to avoid drift.
    """
    conn = _get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  to_regclass('public.github_trending') AS github_trending
                """
            )
            github_trending = cur.fetchone()[0]
            if not github_trending:
                raise RuntimeError(
                    "Missing required table public.github_trending in gahyeonbot DB. "
                    "Apply DB migrations (Flyway) before running this DAG."
                )
            logger.info("Preflight OK: %s exists", github_trending)

        # Ensure the delta-event table exists even if there are 0 new repos today.
        _ensure_events_table(conn)
    finally:
        conn.close()


def compute_new_repos(**context):
    """
    Compare current crawl vs previous snapshot_date and extract newly-added repos.

    NOTE:
    - "신규" 기준은 "직전 스냅샷에 없고 이번엔 있는 repo" (RSS delta 느낌)
    - repo가 나갔다가 다시 들어오면 그날 다시 신규로 잡힐 수 있음
    """
    repos = context["ti"].xcom_pull(task_ids="crawl_trending", key="repos") or []
    if not repos:
        logger.warning("No repos found in XCom; skipping new-repo diff")
        context["ti"].xcom_push(key="new_repo_full_names", value=[])
        return 0

    today = datetime.utcnow().date()
    conn = _get_pg_conn()
    try:
        prev_date = None
        prev_set = set()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT MAX(snapshot_date)
                    FROM github_trending
                    WHERE snapshot_date < %s
                    """,
                    (today,),
                )
                prev_date = cur.fetchone()[0]

                if prev_date:
                    cur.execute(
                        """
                        SELECT repo_full_name
                        FROM github_trending
                        WHERE snapshot_date = %s
                        """,
                        (prev_date,),
                    )
                    prev_set = {r[0] for r in cur.fetchall()}
        except Exception as e:
            # If schema/table isn't available, default to treating everything as "new".
            logger.exception("Failed to load previous snapshot from github_trending; treating all as new: %s", e)
            prev_date = None
            prev_set = set()

        curr_set = {r["repo_full_name"] for r in repos if r.get("repo_full_name")}
        new_names = curr_set - prev_set
        new_repo_full_names = sorted(new_names)

        logger.info(
            "New trending repos: %d (prev snapshot: %s, curr: %d)",
            len(new_repo_full_names),
            str(prev_date) if prev_date else "NONE",
            len(curr_set),
        )
        context["ti"].xcom_push(key="new_repo_full_names", value=new_repo_full_names)
        return len(new_repo_full_names)
    finally:
        conn.close()


def crawl_github_trending(**context):
    """Fetch and parse GitHub Trending page."""
    resp = requests.get(GITHUB_TRENDING_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    articles = soup.find_all("article", class_="Box-row")

    if not articles:
        raise ValueError("No trending repos found - page structure may have changed")

    repos = []
    for article in articles:
        h2 = article.find("h2")
        if not h2:
            continue
        link = h2.find("a")
        if not link:
            continue

        repo_path = link["href"].strip("/")
        repo_url = f"https://github.com/{repo_path}"

        # description
        p = article.find("p")
        description = p.text.strip() if p else None

        # language
        lang_span = article.find("span", attrs={"itemprop": "programmingLanguage"})
        language = lang_span.text.strip() if lang_span else None

        # total stars
        stars_links = article.find_all("a", href=re.compile(r"/stargazers"))
        stars_total = 0
        if stars_links:
            stars_text = stars_links[0].get_text(strip=True)
            stars_total = _parse_count(stars_text)

        # period stars (e.g. "1,234 stars today")
        stars_period = None
        period_span = article.find_all("span", class_="d-inline-block float-sm-right")
        if period_span:
            period_text = period_span[0].get_text(" ", strip=True)
            # "1,234 stars today" / "56 stars this week"
            m = re.search(r"([\d,\.]+)\s+stars", period_text.lower())
            if m:
                stars_period = _parse_count(m.group(1))

        repos.append({
            "repo_full_name": repo_path,
            "repo_url": repo_url,
            "description": description,
            "language": language,
            "stars_total": stars_total,
            "stars_period": stars_period,
        })

    logger.info("Crawled %d trending repos from GitHub", len(repos))
    context["ti"].xcom_push(key="repos", value=repos)
    return len(repos)


def insert_to_db(**context):
    """Insert crawled repos into gahyeonbot's github_trending table."""
    repos = context["ti"].xcom_pull(task_ids="crawl_trending", key="repos")
    if not repos:
        logger.warning("No repos to insert")
        return 0

    today = datetime.utcnow().date()
    now = datetime.utcnow()

    rows = [
        (
            today,
            r["repo_full_name"],
            r["repo_url"],
            r["description"],
            r["language"],
            r["stars_total"],
            r["stars_period"],
            now,
        )
        for r in repos
    ]

    conn = _get_pg_conn()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO github_trending
                    (snapshot_date, repo_full_name, repo_url, description,
                     language, stars_total, stars_period, created_at)
                VALUES %s
                ON CONFLICT (snapshot_date, repo_full_name)
                DO UPDATE SET
                    description = EXCLUDED.description,
                    language = EXCLUDED.language,
                    stars_total = EXCLUDED.stars_total,
                    stars_period = EXCLUDED.stars_period,
                    created_at = EXCLUDED.created_at
                """,
                rows,
            )
        conn.commit()
        logger.info("Inserted/updated %d repos for %s", len(rows), today)
    finally:
        conn.close()

    return len(rows)

def insert_new_events(**context):
    """
    Insert "newly added" events for today.

    This table is meant for downstream consumers (e.g., Discord bot) to poll.
    """
    new_repo_full_names = context["ti"].xcom_pull(
        task_ids="compute_new_repos",
        key="new_repo_full_names",
    ) or []
    if not new_repo_full_names:
        logger.info("No new repos to emit events for today")
        return 0

    repos = context["ti"].xcom_pull(task_ids="crawl_trending", key="repos") or []
    if not repos:
        logger.warning("No repos found in XCom; cannot emit events")
        return 0

    new_set = set(new_repo_full_names)
    new_repos = [r for r in repos if r.get("repo_full_name") in new_set]
    if not new_repos:
        logger.info("No matching repo dicts for new_repo_full_names; skipping event insert")
        return 0

    today = datetime.utcnow().date()
    now = datetime.utcnow()

    rows = [
        (
            today,
            r["repo_full_name"],
            r["repo_url"],
            r.get("description"),
            r.get("language"),
            int(r.get("stars_total") or 0),
            r.get("stars_period"),
            now,
        )
        for r in new_repos
    ]

    conn = _get_pg_conn()
    try:
        _ensure_events_table(conn)
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO github_trending_events
                    (snapshot_date, repo_full_name, repo_url, description,
                     language, stars_total, stars_period, created_at)
                VALUES %s
                ON CONFLICT (snapshot_date, repo_full_name)
                DO UPDATE SET
                    repo_url = EXCLUDED.repo_url,
                    description = EXCLUDED.description,
                    language = EXCLUDED.language,
                    stars_total = EXCLUDED.stars_total,
                    stars_period = EXCLUDED.stars_period
                """,
                rows,
            )
        conn.commit()
        logger.info("Inserted/updated %d github_trending_events for %s", len(rows), today)
        return len(rows)
    finally:
        conn.close()


with DAG(
    dag_id="github_trending_crawler",
    default_args=default_args,
    description="Crawl GitHub Trending daily and store in gahyeonbot DB",
    schedule="0 6 * * *",  # 매일 오전 6시 (UTC) = 오후 3시 KST
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["github", "trending", "gahyeonbot"],
) as dag:

    crawl_task = PythonOperator(
        task_id="crawl_trending",
        python_callable=crawl_github_trending,
    )

    preflight_task = PythonOperator(
        task_id="preflight_gahyeonbot_schema",
        python_callable=preflight_gahyeonbot_schema,
    )

    compute_new_task = PythonOperator(
        task_id="compute_new_repos",
        python_callable=compute_new_repos,
    )

    insert_task = PythonOperator(
        task_id="insert_to_db",
        python_callable=insert_to_db,
    )

    insert_events_task = PythonOperator(
        task_id="insert_new_events",
        python_callable=insert_new_events,
    )

    crawl_task >> preflight_task >> compute_new_task >> insert_task >> insert_events_task
