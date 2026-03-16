import streamlit as st
import requests
import re
import time
import io
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

# ─────────────────────────────────────────────
# PASSWORD GATE
# ─────────────────────────────────────────────

def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.image("https://upload.wikimedia.org/wikipedia/commons/0/08/Pinterest-logo.png", width=60)
        st.title("Pinterest Username Lookup")
        st.markdown("---")
        password = st.text_input("Enter password", type="password", placeholder="Password")

        if st.button("Login", use_container_width=True, type="primary"):
            if password == st.secrets["APP_PASSWORD"]:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("❌ Incorrect password")

    st.stop()

check_password()


# ─────────────────────────────────────────────
# FIX 1: CACHING
# Cache resolved URLs in session_state so the same
# short link is never hit twice in the same session.
# ─────────────────────────────────────────────

# Plain dict cache — lives for the duration of the run
# Can't use st.session_state inside threads (not thread-safe)
url_cache = {}



# ─────────────────────────────────────────────
# FIX 2: BETTER URL PARSING
# Explicitly handles:
#   - Country/locale subdomains (in., uk., au., etc.)
#   - /pin/ content URLs → not a profile
#   - /search/, /explore/, /ideas/ → not a profile
#   - /_/ locale prefix (e.g. pinterest.com/_/search/)
#   - Two-segment paths like /username/board/ → extract only username
# ─────────────────────────────────────────────

# These are Pinterest path prefixes that are NOT usernames
NON_PROFILE_PATHS = {
    "pin", "search", "explore", "ideas", "login", "logout",
    "settings", "notifications", "inbox", "shop", "today",
    "business", "help", "about", "_", "static"
}

def extract_username(full_url: str) -> str:
    """
    Robustly extracts Pinterest username from any Pinterest URL format.

    Handles:
      https://www.pinterest.com/username/          → username
      https://in.pinterest.com/username/           → username (country subdomain)
      https://www.pinterest.com/username/boardname → username (ignores board segment)
      https://www.pinterest.com/pin/123456/        → ERROR (content URL, not profile)
      https://pin.it/xyz (unresolved)              → ERROR (should be resolved first)
    """
    if full_url.startswith("ERROR"):
        return full_url

    try:
        parsed = urlparse(full_url)

        # Confirm it's actually a Pinterest domain
        # Strips subdomains: www., in., uk., au., etc.
        hostname = parsed.hostname or ""
        if not hostname.endswith("pinterest.com"):
            return "ERROR: Not a Pinterest URL"

        # Split path into clean segments, removing empty strings
        # e.g. "/username/board/" → ["username", "board"]
        path_segments = [s for s in parsed.path.split("/") if s]

        if not path_segments:
            return "ERROR: No path segments found"

        # First segment is always the username candidate
        candidate = path_segments[0]

        # Reject known non-profile paths
        if candidate.lower() in NON_PROFILE_PATHS:
            return f"ERROR: Non-profile path (/{candidate}/)"

        # Reject pure numeric segments (e.g. pin IDs)
        if candidate.isdigit():
            return "ERROR: Numeric path segment — not a username"

        return candidate

    except Exception as e:
        return f"ERROR: URL parsing failed — {e}"


# ─────────────────────────────────────────────
# CORE: RESOLVE SHORT LINK (with caching)
# ─────────────────────────────────────────────

def resolve_short_link(short_url: str) -> str:
    # Cache hit — return immediately
    if short_url in url_cache:
        return url_cache[short_url]

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    try:
        response = requests.get(short_url, allow_redirects=True, timeout=10, headers=headers)
        response.raise_for_status()
        result = response.url
    except requests.RequestException as e:
        result = f"ERROR: {e}"

    # Store in cache
    url_cache[short_url] = result
    return result



# ─────────────────────────────────────────────
# FIX 3: PARALLEL PROCESSING
# Uses ThreadPoolExecutor to resolve multiple URLs concurrently.
# MAX_WORKERS controls parallelism — kept at 5 to avoid
# hammering Pinterest and triggering rate limits.
# delay_between_batches adds a pause between each parallel batch.
# ─────────────────────────────────────────────

MAX_WORKERS = 5  # Number of concurrent requests

def process_single(short_url: str) -> dict:
    """
    Processes one URL — resolve + extract username.
    Designed to be called in parallel via ThreadPoolExecutor.
    """
    short_url = short_url.strip()

    if not short_url.lower().startswith("http"):
        return {
            "short_url": short_url,
            "full_url":  "SKIPPED",
            "username":  "SKIPPED",
            "status":    "SKIPPED",
        }

    full_url = resolve_short_link(short_url)
    username = extract_username(full_url)
    status   = "OK" if not username.startswith("ERROR") else "FAILED"

    return {
        "short_url": short_url,
        "full_url":  full_url,
        "username":  username,
        "status":    status,
    }


def process_links(short_urls: list, delay: float = 1.0, progress_bar=None, status_text=None) -> list:
    """
    Processes all URLs in parallel batches of MAX_WORKERS.

    Why batches instead of all at once:
      - Sending 100 requests simultaneously would likely trigger Pinterest rate limiting
      - Batching gives us parallelism within a controlled rate cap
      - delay applies between batches, not between individual requests

    Example with 20 URLs, MAX_WORKERS=5, delay=1.0:
      Batch 1: URLs 1-5  fired simultaneously → wait 1s
      Batch 2: URLs 6-10 fired simultaneously → wait 1s
      ... and so on
      Total time ≈ 4 batches × 1s = ~4s instead of 20s sequential
    """
    total = len(short_urls)
    results_map = {}  # Use dict to preserve original order

    # Split into batches of MAX_WORKERS
    batches = [short_urls[i:i + MAX_WORKERS] for i in range(0, total, MAX_WORKERS)]
    completed = 0

    for batch in batches:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all URLs in this batch concurrently
            future_to_url = {executor.submit(process_single, url): url for url in batch}

            for future in as_completed(future_to_url):
                result = future.result()
                results_map[result["short_url"]] = result
                completed += 1

                if progress_bar:
                    progress_bar.progress(completed / total)
                if status_text:
                    status_text.text(f"Processing {completed}/{total}: {result['short_url']}")

        # Polite pause between batches
        if batch != batches[-1]:
            time.sleep(delay)

    # Return results in original input order
    return [results_map[url.strip()] for url in short_urls if url.strip() in results_map]


# ─────────────────────────────────────────────
# SQL BUILDER
# ─────────────────────────────────────────────

def build_in_clause(results: list) -> str:
    usernames = [
        f"'{r['username'].lower()}'"
        for r in results
        if r["status"] == "OK"
    ]
    if not usernames:
        return "-- No valid usernames found"

    in_clause = ",\n        ".join(usernames)
    return f"""SELECT
    id AS user_id,
    json_extract_scalar(json, '$.username') AS username
FROM
    default.pii_db_users
WHERE
    json_extract_scalar(json, '$.username') IN (
        {in_clause}
    )
;"""


# ─────────────────────────────────────────────
# STREAMLIT UI
# ─────────────────────────────────────────────

st.set_page_config(
    page_title="Pinterest Username Lookup",
    page_icon="📌",
    layout="centered"
)

st.title("📌 Pinterest Username Lookup")
st.markdown("Upload a CSV of Pinterest short links to extract usernames and generate a Querybook SQL query.")
st.divider()

# ── Input method toggle ──
input_method = st.radio(
    "How would you like to input links?",
    ["Upload CSV file", "Paste links manually"],
    horizontal=True
)

short_urls = []

if input_method == "Upload CSV file":
    uploaded_file = st.file_uploader(
        "Upload your CSV file",
        type=["csv"],
        help="CSV should have one Pinterest link per row. Header row is optional."
    )

    if uploaded_file:
        content = uploaded_file.read().decode("utf-8")
        lines = [line.strip() for line in content.splitlines() if line.strip()]

        if lines and not lines[0].lower().startswith("http"):
            st.caption(f"⏭️ Skipping header row: `{lines[0]}`")
            lines = lines[1:]

        short_urls = [line.split(",")[0].strip() for line in lines]
        st.success(f"✅ Loaded {len(short_urls)} links from CSV")

else:
    pasted = st.text_area(
        "Paste Pinterest links (one per line)",
        placeholder="https://pin.it/wosERPeg4\nhttps://pin.it/abc1234\nhttps://in.pinterest.com/queenoftreasures/",
        height=150
    )
    if pasted.strip():
        short_urls = [line.strip() for line in pasted.splitlines() if line.strip()]
        st.success(f"✅ Detected {len(short_urls)} links")

# ── Preview input ──
if short_urls:
    with st.expander(f"Preview input links ({len(short_urls)} total)"):
        for url in short_urls:
            st.text(url)

# ── Run button ──
if short_urls:
    delay = st.slider(
        "Delay between batches (seconds)",
        min_value=0.5, max_value=3.0, value=1.0, step=0.5,
        help="Pause between parallel batches of 5 requests — increase if you hit rate limits"
    )

    if st.button("🚀 Run Lookup", type="primary", use_container_width=True):

        st.divider()

        progress_bar = st.progress(0)
        status_text  = st.empty()

        results = process_links(short_urls, delay=delay, progress_bar=progress_bar, status_text=status_text)

        progress_bar.empty()
        status_text.empty()

        # ── Summary metrics ──
        ok_count      = sum(1 for r in results if r["status"] == "OK")
        failed_count  = sum(1 for r in results if r["status"] == "FAILED")
        skipped_count = sum(1 for r in results if r["status"] == "SKIPPED")

        col1, col2, col3 = st.columns(3)
        col1.metric("✅ Resolved", ok_count)
        col2.metric("❌ Failed",   failed_count)
        col3.metric("⏭️ Skipped",  skipped_count)

        st.divider()

        # ── Results table ──
        st.subheader("Results")
        df = pd.DataFrame(results)[["short_url", "username", "status"]]

        def color_status(val):
            colors = {
                "OK":      "background-color: #d4edda",
                "FAILED":  "background-color: #f8d7da",
                "SKIPPED": "background-color: #fff3cd"
            }
            return colors.get(val, "")

        st.dataframe(
            df.style.applymap(color_status, subset=["status"]),
            use_container_width=True,
            hide_index=True
        )

        st.divider()

        # ── SQL IN clause ──
        st.subheader("Querybook SQL")
        sql = build_in_clause(results)
        st.code(sql, language="sql")
        st.caption("👆 Click the copy icon in the top right of the code block to copy SQL")

        st.divider()

        # ── Download CSV ──
        st.subheader("Download Results")
        output_df = pd.DataFrame(results)
        csv_buffer = io.StringIO()
        output_df.to_csv(csv_buffer, index=False)

        st.download_button(
            label="⬇️ Download CSV",
            data=csv_buffer.getvalue(),
            file_name="output_usernames.csv",
            mime="text/csv",
            use_container_width=True
        )
