#!/usr/bin/env python3
"""
OPM Data Downloader
Downloads all Employment, Accessions, Separations files (2005-2025)
Run: python opm_downloader.py "/Volumes/T7 Shield/opm_data"
"""

import re, sys, time
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("pip install playwright && playwright install chromium")
    sys.exit(1)

#  Config 
BASE_DIR   = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("opm_data")
URL        = "https://data.opm.gov/explore-data/data/data-downloads"
CATEGORIES = ["Employment", "Accessions", "Separations"]
DELAY      = 2  # seconds between downloads

MONTH_MAP = {
    "january":"01","february":"02","march":"03","april":"04",
    "may":"05","june":"06","july":"07","august":"08",
    "september":"09","october":"10","november":"11","december":"12"
}

def get_filename(source, date_label, version):
    """Build filename like: employment_202307_1.txt"""
    m = re.match(r'(\w+)\s+(\d{4})', date_label)
    if m:
        month = MONTH_MAP.get(m.group(1).lower())
        year  = m.group(2)
        if month:
            return f"{source.lower()}_{year}{month}_{version}.txt"
    return None

def scrape_page(page):
    """Extract all file records from current page."""
    return page.evaluate("""
        () => {
            const results = [];
            const headers = document.querySelectorAll('h1,h2,h3,h4,h5,h6,[class*="title"]');
            for (const h of headers) {
                const text = h.textContent?.trim() || '';
                const m = text.match(/Federal\\s+(Employment|Accessions|Separations)\\s+Raw\\s+Data\\s*\\(([^)]+)\\)/i);
                if (m) {
                    const container = h.closest('[class*="card"]') || h.parentElement?.parentElement;
                    const cText     = container?.textContent || '';
                    const ver       = (cText.match(/Version:\\s*(\\d+)/i) || [])[1] || '1';
                    const isCurrent = cText.includes('Current');
                    results.push({
                        source:     m[1],
                        dateLabel:  m[2].trim(),
                        version:    ver,
                        isCurrent:  isCurrent
                    });
                }
            }
            return results;
        }
    """)

def click_next(page):
    """Click next page button. Returns True if successful."""
    try:
        btn = page.locator('button[aria-label*="next" i]').first
        if btn.is_visible() and btn.is_enabled():
            btn.click()
            time.sleep(4)
            return True
    except Exception:
        pass
    return False

def download_file(page, context, save_path, record):
    """Find and click the download button for a specific record."""
    try:
        # Find the card matching this record
        title = f"Federal {record['source']} Raw Data ({record['dateLabel']})"
        card  = page.locator(f'text="{title}"').first
        if not card.is_visible():
            return False

        # Click download button inside that card
        container   = card.locator('xpath=ancestor::*[contains(@class,"card")][1]')
        dl_button   = container.locator('button:has-text("Download")').first

        with context.expect_download() as dl_info:
            dl_button.click()

        download = dl_info.value
        fname    = get_filename(record['source'], record['dateLabel'], record['version'])
        if fname:
            download.save_as(save_path / fname)
            return fname
    except Exception as e:
        print(f"    [!] Error: {e}")
    return False

def main():
    # Create folders
    for cat in CATEGORIES:
        (BASE_DIR / cat.lower()).mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page    = context.new_page()
        page.set_viewport_size({"width": 1400, "height": 900})

        total_downloaded = 0

        for category in CATEGORIES:
            print(f"\n{'='*50}")
            print(f"Category: {category}")
            print(f"{'='*50}")

            save_path = BASE_DIR / category.lower()

            #  Load page 
            page.goto(URL, wait_until="networkidle", timeout=120000)
            time.sleep(5)

            #   Set 100 rows per page 
            try:
                dropdown = page.locator('select').last
                if dropdown.is_visible():
                    dropdown.select_option("100")
                    time.sleep(3)
            except Exception:
                print("  Could not set 100 rows, using default")

            #   Filter by category  
            try:
                page.select_option("select[name='dataSource']", category)
                time.sleep(2)
            except Exception:
                print(f"  Could not filter by {category}")

            #   Filter current version only  
            try:
                page.select_option("select[name='dataVersion']", "Current version")
                time.sleep(2)
            except Exception:
                pass

            page_num      = 0
            cat_count     = 0
            empty_pages   = 0

            #   Loop through all pages   
            while True:
                page_num += 1
                print(f"\n  Page {page_num}...")
                time.sleep(2)

                records = scrape_page(page)

                if not records:
                    empty_pages += 1
                    if empty_pages >= 2:
                        print("  No more records found.")
                        break
                    continue

                empty_pages = 0
                print(f"  Found {len(records)} records")

                #   Download each file on this page  
                for rec in records:
                    # Skip already downloaded files
                    fname = get_filename(rec['source'], rec['dateLabel'], rec['version'])
                    if fname and (save_path / fname).exists():
                        print(f"    [skip] {fname} already exists")
                        continue

                    result = download_file(page, context, save_path, rec)
                    if result:
                        cat_count     += 1
                        total_downloaded += 1
                        print(f"    [✓] {result}  ({cat_count} done)")
                    else:
                        print(f"    [✗] Failed: {rec['source']} {rec['dateLabel']}")

                    time.sleep(DELAY)

                #   Next page     
                if not click_next(page):
                    print(f"\n  Last page reached for {category}")
                    break

                if page_num > 50:  # safety limit
                    print("  Safety limit reached")
                    break

            print(f"\n  {category} done — {cat_count} files downloaded")

        browser.close()
        print(f"\n{'='*50}")
        print(f"All done! Total files downloaded: {total_downloaded}")
        print(f"   Saved to: {BASE_DIR}")

if __name__ == "__main__":
    main()