import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup, SoupStrainer # Use SoupStrainer for efficiency
import csv
import json
import logging
import os
import re
import time
from urllib.parse import urlparse, urljoin
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from aiohttp import ClientSession, ClientError, TCPConnector
from tqdm.asyncio import tqdm

# --- Configuration ---
INPUT_CSV_FILE = 'libraries_over_60k.csv' # From the first script
OUTPUT_CSV_FILE = 'libraries_with_suggestion_links.csv' # New output file
CHECKPOINT_PROCESSED_WEBSITES = 'processed_websites_suggestions.json' # Track processed sites

MAX_CONCURRENT_REQUESTS = 5
DELAY_PER_REQUEST = 0.2
MAX_RETRIES = 3
INITIAL_BACKOFF = 2
MAX_CRAWL_DEPTH = 1 # 0 = homepage only, 1 = homepage + links on homepage
HEADERS = {
    'User-Agent': 'LibraryLinkFinder/1.0 (Contact: Chad Nygard - [Your Email Here])', # Update email
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

# --- Keyword Scoring ---
KEYWORD_SCORES = {
    10: ['suggest', 'recommend', 'purchase', 'acquisition', 'request', 'order a book', 'ask a librarian to buy'], # Top Tier
    8: ['contact', 'ask', 'form', 'librarian', 'feedback', 'question', 'reach us'], # High Tier (Contact)
    5: ['collection development', 'materials selection', 'local author', 'services', 'about', 'help'], # Medium Tier (Context)
    -5: ['login', 'catalog', 'hours', 'events', 'facebook', 'twitter', 'instagram', 'youtube', 'donate', 'support', 'friends', 'jobs', 'employment', 'careers', 'privacy', 'terms'] # Negative Tier
}

# Pre-compile regex for efficiency
KEYWORD_REGEX = {}
for score, words in KEYWORD_SCORES.items():
    # Use word boundaries (\b) to match whole words, case-insensitive (?i)
    KEYWORD_REGEX[score] = re.compile(r'\b(' + '|'.join(re.escape(w) for w in words) + r')\b', re.IGNORECASE)

# Setup Logging
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
if not logger.handlers:
    logger.setLevel(logging.INFO)
    # File handler
    file_handler = logging.FileHandler('link_finder.log', mode='a', encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)


# --- Helper Functions (Adapted from previous script) ---

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=INITIAL_BACKOFF, max=10),
    retry=retry_if_exception_type((ClientError, TimeoutError, asyncio.TimeoutError)),
    before_sleep=lambda retry_state: logging.warning(
        f"Retrying {retry_state.fn.__name__} for URL {retry_state.args[1]} due to {retry_state.outcome.exception()}, attempt {retry_state.attempt_number}"
    )
)
async def fetch_page(session: ClientSession, url: str) -> str | None:
    """Fetches HTML content for a given URL with retries and error handling."""
    # Skip non-http URLs immediately
    if not url or not url.lower().startswith(('http://', 'https://')):
        logging.warning(f"Skipping invalid or non-HTTP URL: {url}")
        return None
    await asyncio.sleep(DELAY_PER_REQUEST)
    try:
        timeout = aiohttp.ClientTimeout(total=20, connect=10) # Shorter timeout for link finding
        async with session.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True, max_redirects=5) as response:
            if response.status == 429:
                retry_after = int(response.headers.get("Retry-After", "5"))
                logging.warning(f"Rate limit hit (429) for {url}, waiting {retry_after} seconds")
                await asyncio.sleep(retry_after)
                raise ClientError(f"Rate limit hit (429) for {url}")
            # Only process successful HTML responses
            if response.status >= 400:
                 logging.warning(f"HTTP error {response.status} fetching {url}")
                 return None # Don't retry client/server errors here unless specified in retry decorator
            if 'text/html' not in response.headers.get('Content-Type', '').lower():
                 logging.info(f"Skipping non-HTML content at {url} (Content-Type: {response.headers.get('Content-Type')})")
                 return None

            logging.info(f"Successfully fetched: {url} (Status: {response.status})")
            try:
                 charset = response.charset or 'utf-8' # Default to utf-8 if not detected
                 return await response.text(encoding=charset, errors='ignore')
            except UnicodeDecodeError:
                 logging.warning(f"Decode failed for {url} with charset {charset}, trying windows-1252")
                 return await response.text(encoding='windows-1252', errors='ignore')

    except ClientError as e:
        status_code = e.status if hasattr(e, 'status') else 'N/A'
        logging.error(f"Client error fetching {url}: Status {status_code}, Message: {e}")
        raise # Re-raise for tenacity
    except asyncio.TimeoutError:
        logging.error(f"Timeout fetching {url}")
        raise # Re-raise for tenacity
    except Exception as e:
        logging.exception(f"Unexpected error fetching {url}: {e}")
        return None

async def save_json(data, filename: str):
    """Asynchronously saves data to a JSON file."""
    try:
        async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(data, indent=2))
    except IOError as e:
        logging.error(f"Error writing JSON checkpoint {filename}: {e}")

async def load_json(filename: str) -> dict:
    """Asynchronously loads data from a JSON file."""
    if os.path.exists(filename):
        try:
            async with aiofiles.open(filename, 'r', encoding='utf-8') as f:
                content = await f.read()
                if not content:
                    logging.warning(f"Checkpoint file {filename} is empty.")
                    return {}
                return json.loads(content)
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from {filename}. Returning default.")
            return {}
        except IOError as e:
            logging.error(f"Error reading JSON checkpoint {filename}: {e}")
            return {}
    return {} # Default if file doesn't exist

# --- Link Scoring ---
def score_link(link_text: str, link_url: str) -> int:
    """Calculates a relevance score for a link based on keywords."""
    score = 0
    text = link_text.lower()
    url_path = urlparse(link_url).path.lower()

    # Apply positive scores
    for points, regex in KEYWORD_REGEX.items():
        if points > 0:
            if regex.search(text) or regex.search(url_path):
                score += points

    # Apply negative scores (only if some positive score was already assigned)
    if score > 0:
        if KEYWORD_REGEX[-5].search(text) or KEYWORD_REGEX[-5].search(url_path):
            score += -5 # Apply penalty

    # Boost score slightly if keywords appear in both text and URL
    if score > 0:
         for points, regex in KEYWORD_REGEX.items():
              if points > 0 and regex.search(text) and regex.search(url_path):
                   score += 1 # Small boost for matching both

    return score

# --- Core Logic ---
async def process_library_website(session: ClientSession, library_data: dict, semaphore: asyncio.Semaphore):
    """Crawls a library website (limited depth) and finds the best suggestion link."""
    base_url = library_data.get('Website')
    library_name = library_data.get('Name', 'Unknown Library')
    result_data = library_data.copy() # Start with original data
    result_data['SuggestionLinkGuess'] = "Not Found" # Default

    if not base_url or base_url == 'Website Not Found' or not base_url.startswith('http'):
        logging.info(f"Skipping {library_name} due to missing/invalid website URL: {base_url}")
        return result_data # Return original data with "Not Found"

    try:
        parsed_base_url = urlparse(base_url)
        base_domain = parsed_base_url.netloc
    except ValueError:
        logging.warning(f"Could not parse base URL for {library_name}: {base_url}")
        return result_data

    visited_urls = set()
    candidate_links = [] # Store tuples: (score, link_text, link_url)
    queue = asyncio.Queue()

    # Normalize base_url before adding
    normalized_base_url = urljoin(base_url, '/') # Ensure it has a trailing slash if root
    await queue.put((normalized_base_url, 0)) # Add (url, depth)

    async with semaphore: # Limit concurrency for this entire website process
        while not queue.empty():
            try:
                current_url, current_depth = await queue.get()
                queue.task_done()

                if current_depth > MAX_CRAWL_DEPTH:
                    continue

                # Normalize URL for visited check
                normalized_url = urljoin(current_url, urlparse(current_url).path).lower()
                if normalized_url in visited_urls:
                    continue

                # Check domain
                try:
                    current_domain = urlparse(current_url).netloc
                    if current_domain != base_domain:
                         logging.debug(f"Skipping external link: {current_url}")
                         continue
                except ValueError:
                     logging.warning(f"Could not parse domain for {current_url}, skipping.")
                     continue

                visited_urls.add(normalized_url)
                logging.debug(f"Processing (Depth {current_depth}): {current_url}")

                html = await fetch_page(session, current_url)
                if not html:
                    continue # Skip page if fetch failed

                # Use SoupStrainer to only parse 'a' tags for efficiency
                strainer = SoupStrainer('a', href=True)
                soup = BeautifulSoup(html, 'html.parser', parse_only=strainer)

                for link_tag in soup.find_all('a'):
                    href = link_tag.get('href')
                    link_text = link_tag.get_text(strip=True)
                    if not href or not link_text: # Skip links without href or text
                        continue

                    try:
                        absolute_url = urljoin(current_url, href) # Resolve relative URLs
                        # Basic check to ignore mailto, javascript, etc.
                        if not absolute_url.lower().startswith(('http://', 'https://')):
                            continue
                    except ValueError:
                        logging.warning(f"Could not join URL: base='{current_url}', href='{href}'")
                        continue

                    # Score the link
                    score = score_link(link_text, absolute_url)
                    if score > 0:
                        candidate_links.append((score, link_text, absolute_url))
                        logging.debug(f"  Found candidate: '{link_text}' ({absolute_url}) - Score: {score}")

                    # Add internal links to queue if within depth limit
                    if current_depth < MAX_CRAWL_DEPTH:
                        try:
                            link_domain = urlparse(absolute_url).netloc
                            normalized_link = urljoin(absolute_url, urlparse(absolute_url).path).lower()
                            if link_domain == base_domain and normalized_link not in visited_urls:
                                # Basic check to avoid adding duplicates already in queue implicitly
                                # A more robust check might involve querying the queue state if needed
                                await queue.put((absolute_url, current_depth + 1))
                                logging.debug(f"  Queueing internal link: {absolute_url}")
                        except ValueError:
                             logging.warning(f"Could not parse domain for link {absolute_url}, not queueing.")
                             continue

            except asyncio.CancelledError:
                 logging.warning(f"Task cancelled while processing {base_url}")
                 break # Exit loop if task is cancelled
            except Exception as e:
                 logging.exception(f"Error in crawl loop for {base_url}: {e}")
                 # Continue processing other items in queue if possible

        # Process candidates after crawl finishes
        if candidate_links:
            candidate_links.sort(key=lambda x: x[0], reverse=True) # Sort by score descending
            best_score, best_text, best_url = candidate_links[0]
            result_data['SuggestionLinkGuess'] = best_url
            logging.info(f"Best guess for {library_name}: '{best_text}' ({best_url}) - Score: {best_score}")
        else:
            logging.info(f"No suitable suggestion/contact link found for {library_name}")

    return result_data


# --- Main Orchestration ---
async def main():
    """Main function to orchestrate the link finding process."""
    start_time = time.time()
    logging.info("--- Starting Suggestion Link Finder ---")

    # Load the initial library data
    libraries_to_process = []
    if not os.path.exists(INPUT_CSV_FILE):
        logging.critical(f"Input CSV file '{INPUT_CSV_FILE}' not found. Please run the first scraper script.")
        return
    try:
        with open(INPUT_CSV_FILE, 'r', encoding='utf-8', newline='') as csvfile:
             reader = csv.DictReader(csvfile)
             if not reader.fieldnames or 'Website' not in reader.fieldnames:
                  logging.critical(f"Input CSV '{INPUT_CSV_FILE}' is missing headers or the 'Website' column.")
                  return
             libraries_to_process = list(reader)
        logging.info(f"Read {len(libraries_to_process)} libraries from {INPUT_CSV_FILE}")
    except Exception as e:
         logging.exception(f"Failed to read input CSV {INPUT_CSV_FILE}: {e}")
         return

    # Load checkpoint of already processed websites
    processed_data = await load_json(CHECKPOINT_PROCESSED_WEBSITES)
    processed_websites_set = set(processed_data.get('processed_websites', []))
    logging.info(f"Loaded {len(processed_websites_set)} previously processed websites from {CHECKPOINT_PROCESSED_WEBSITES}")

    # Filter out libraries whose websites have already been processed
    tasks_to_run_data = [lib for lib in libraries_to_process if lib.get('Website') and lib.get('Website') not in processed_websites_set and lib.get('Website') != 'Website Not Found']
    logging.info(f"Processing {len(tasks_to_run_data)} new/remaining library websites.")

    if not tasks_to_run_data:
        logging.info("No new websites to process.")
        end_time = time.time()
        logging.info(f"--- Suggestion Link Finder finished in {end_time - start_time:.2f} seconds ---")
        return

    # Prepare output CSV (write header if needed)
    output_fieldnames = list(libraries_to_process[0].keys()) + ['SuggestionLinkGuess'] if libraries_to_process else []
    if output_fieldnames and not (os.path.exists(OUTPUT_CSV_FILE) and os.path.getsize(OUTPUT_CSV_FILE) > 0):
         try:
              with open(OUTPUT_CSV_FILE, 'w', encoding='utf-8', newline='') as f:
                   writer = csv.DictWriter(f, fieldnames=output_fieldnames, quoting=csv.QUOTE_ALL)
                   writer.writeheader()
              logging.info(f"Created output file {OUTPUT_CSV_FILE} with headers.")
         except IOError as e:
              logging.error(f"Could not write header to {OUTPUT_CSV_FILE}: {e}")
              return


    # Run processing concurrently
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    conn = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS * 2, limit_per_host=MAX_CONCURRENT_REQUESTS)
    processed_count = 0
    batch_size = 50 # Write results to CSV in batches

    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [process_library_website(session, library_data, semaphore) for library_data in tasks_to_run_data]
        results_batch = []

        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Finding links", unit="website"):
            try:
                result = await future
                if result: # process_library_website should always return the dict
                    results_batch.append(result)
                    website_url = result.get('Website')
                    if website_url:
                        processed_websites_set.add(website_url)

                    # Write batch and update checkpoint periodically
                    if len(results_batch) >= batch_size:
                        # Write CSV batch (Ensure append_csv handles dicts correctly)
                        await append_csv(results_batch, OUTPUT_CSV_FILE)
                        logging.info(f"Appended batch of {len(results_batch)} results to {OUTPUT_CSV_FILE}")
                        # Save processed websites checkpoint
                        await save_json({'processed_websites': sorted(list(processed_websites_set))}, CHECKPOINT_PROCESSED_WEBSITES)
                        logging.info(f"Saved processed websites checkpoint ({len(processed_websites_set)} total)")
                        results_batch = [] # Reset batch

            except Exception as e:
                logging.exception(f"Error processing future in main loop: {e}")

        # Append any remaining results
        if results_batch:
            await append_csv(results_batch, OUTPUT_CSV_FILE)
            logging.info(f"Appended final batch of {len(results_batch)} results to {OUTPUT_CSV_FILE}")

        # Final save of processed websites
        await save_json({'processed_websites': sorted(list(processed_websites_set))}, CHECKPOINT_PROCESSED_WEBSITES)
        logging.info(f"Final save of processed websites checkpoint ({len(processed_websites_set)} total).")

    end_time = time.time()
    logging.info(f"--- Suggestion Link Finder finished in {end_time - start_time:.2f} seconds ---")


if __name__ == '__main__':
    if os.name == 'nt':
       # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
       pass
    asyncio.run(main())
