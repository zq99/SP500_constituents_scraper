
"""
S&P 500 Constituents Scraper

Downloads the current S&P 500 constituents table from Wikipedia
and saves it as sp500list.csv in the same directory as this script.

Dependencies:
    pip install requests beautifulsoup4 pandas
"""

import logging
import os
import tempfile
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = SCRIPT_DIR / "sp500list.csv"

WIKIPEDIA_URL = (
    "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
)

USER_AGENT = (
    "SP500ConstituentsScraper/1.0 "
    "(https://github.com/zq99/SP500_constituents_scraper)"
)

REQUEST_TIMEOUT = 30

COLUMNS = [
    "symbol",
    "security",
    "gics_sector",
    "sub_industry",
    "headquarters",
    "date_added",
    "cik",
    "founded",
]

logger = logging.getLogger(__name__)


# ------------------------------------------------------------
# Download
# ------------------------------------------------------------

def fetch_wikipedia_html() -> str:
    """Download the Wikipedia page containing S&P 500 constituents."""

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html",
    }

    logger.info("Fetching S&P 500 constituents from Wikipedia...")

    response = requests.get(
        WIKIPEDIA_URL,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )

    response.raise_for_status()

    return response.text


# ------------------------------------------------------------
# Parse
# ------------------------------------------------------------

def parse_constituents(html: str) -> pd.DataFrame:
    """Extract the S&P 500 constituents table into a DataFrame."""

    soup = BeautifulSoup(html, "html.parser")

    table = soup.find("table", id="constituents")

    if table is None:
        raise ValueError(
            "S&P 500 constituents table not found. "
            "Wikipedia may have changed its page structure "
            "or rejected the request."
        )

    records = []

    for tr in table.find_all("tr"):

        cells = tr.find_all("td", recursive=False)

        # Skip header rows.
        if not cells:
            continue

        if len(cells) != len(COLUMNS):
            raise ValueError(
                "Unexpected Wikipedia table structure. "
                f"Expected {len(COLUMNS)} columns, "
                f"found {len(cells)}."
            )

        record = [
            cell.get_text(" ", strip=True)
            for cell in cells
        ]

        records.append(record)

    if not records:
        raise ValueError(
            "No S&P 500 constituent records were extracted."
        )

    df = pd.DataFrame(
        records,
        columns=COLUMNS,
        dtype=str,
    )

    # Preserve identifiers, including leading zeros in CIK.
    df["cik"] = df["cik"].str.strip()

    # Basic validation.
    if df["symbol"].eq("").any():
        raise ValueError("One or more constituent symbols are empty.")

    if df["symbol"].duplicated().any():
        duplicates = df.loc[
            df["symbol"].duplicated(), "symbol"
        ].tolist()

        raise ValueError(
            f"Duplicate constituent symbols detected: {duplicates}"
        )

    logger.info("Extracted %d constituents.", len(df))

    return df


# ------------------------------------------------------------
# Save
# ------------------------------------------------------------

def save_csv(df: pd.DataFrame) -> None:
    """
    Save the CSV alongside this script.

    Uses a temporary file to avoid corrupting an existing
    CSV if the write operation fails.
    """

    temporary_path = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8-sig",
            newline="",
            suffix=".csv",
            prefix=".sp500_",
            dir=SCRIPT_DIR,
            delete=False,
        ) as temporary_file:

            temporary_path = Path(temporary_file.name)

            df.to_csv(
                temporary_file,
                index=False,
            )

        # Replace the existing CSV only after a successful write.
        os.replace(temporary_path, OUTPUT_FILE)

    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    logger.info(
        "Saved %d constituents to: %s",
        len(df),
        OUTPUT_FILE,
    )


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def create_sp500_file() -> bool:
    """Download, validate and save the S&P 500 constituent list."""

    try:
        html = fetch_wikipedia_html()

        df = parse_constituents(html)

        save_csv(df)

        return True

    except requests.RequestException as exc:
        logger.error("Wikipedia download failed: %s", exc)

    except ValueError as exc:
        logger.error("Data validation failed: %s", exc)

    except OSError as exc:
        logger.error("Unable to save CSV: %s", exc)

    return False


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    success = create_sp500_file()

    raise SystemExit(0 if success else 1)
