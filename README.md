# S&P 500 Constituents Scraper

A lightweight Python script that retrieves the latest S&P 500 constituents from Wikipedia and exports them to a CSV file.

## Features

- Scrapes the current S&P 500 constituents table from Wikipedia.
- Exports company information to a structured CSV file.
- Saves `sp500list.csv` automatically in the same directory as the script.
- Preserves company identifiers (CIK) as strings.
- Handles HTTP errors, missing tables and unexpected data structures.
- Uses atomic file replacement to protect existing CSV data if an update fails.

## Installation

Clone the repository:

```bash
git clone https://github.com/zq99/SP500_constituents_scraper.git
cd SP500_constituents_scraper
```

Install the dependencies:

```bash
pip install -r requirements.txt
```

**Requirements:** Python 3.10 or later, pandas, requests and BeautifulSoup4.

## Usage

Run the script:

```bash
python main.py
```

The script downloads the current constituents list and creates or updates `sp500list.csv` in the same directory as `main.py`.

Example console output:

```text
INFO: Fetching S&P 500 constituents from Wikipedia...
INFO: Extracted 503 constituents.
INFO: Saved 503 constituents to: C:\path\to\SP500_constituents_scraper\sp500list.csv
```

The number of constituents may vary as the index changes.

## CSV Output

The generated CSV contains the following columns:

| Column | Description |
|---|---|
| `symbol` | Stock ticker symbol |
| `security` | Company name |
| `gics_sector` | GICS sector |
| `sub_industry` | GICS sub-industry |
| `headquarters` | Headquarters location |
| `date_added` | Date added to the index |
| `cik` | SEC Central Index Key |
| `founded` | Year or date founded |

The output file is overwritten only after the new data has been downloaded, parsed and validated successfully.

## Data Source

[Wikipedia — List of S&P 500 companies](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies)

Data is retrieved on demand. Wikipedia's constituents list is community-maintained and should not be treated as an authoritative historical record or a real-time market-data feed.

## License

This project is licensed under the [MIT License](LICENSE).
