import asyncio
import aiohttp
import aiofiles # For async file operations
from bs4 import BeautifulSoup
import csv
import io # Needed for csv writing with async file handle
import json
import logging
import os
import re
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from aiohttp import ClientSession, ClientError, TCPConnector
from tqdm.asyncio import tqdm # Use tqdm.asyncio for async loops

# --- Configuration ---
BASE_URL = 'https://librarytechnology.org'
MAIN_URL = f'{BASE_URL}/libraries/uspublic/'
TARGET_POPULATION = 60000
MAX_CONCURRENT_REQUESTS = 5  # Start conservatively, increase if stable
DELAY_PER_REQUEST = 0.2      # Seconds delay between requests within a worker
MAX_RETRIES = 3
INITIAL_BACKOFF = 2 # Seconds for first retry delay
HEADERS = {
    'User-Agent': 'LibraryScraper/1.0 (Contact: Chad Nygard - [Your Email Here])', # Be polite! Update email
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br', # Allow compressed responses
    'Connection': 'keep-alive',
}

# Checkpoint file names
CHECKPOINT_STATE_URLS = 'state_urls.json'
CHECKPOINT_PROCESSED_STATES = 'processed_states.json'
CHECKPOINT_LIBRARY_URLS = 'library_urls_temp.jsonl' # Using JSON Lines for easier appending
CHECKPOINT_OUTPUT = 'output_temp.csv'
FINAL_OUTPUT = 'libraries_over_60k.csv'
PROCESSED_LIBS_CHECKPOINT = 'processed_libs.json' # To track processed library URLs

# Setup Logging (to file and console)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
# Prevent adding multiple handlers if script is re-run in some environments
if not logger.handlers:
    logger.setLevel(logging.INFO)
    # File handler
    file_handler = logging.FileHandler('scraper.log', mode='a', encoding='utf-8') # Specify encoding
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)


# --- Helper Functions ---

# Retry decorator for network requests
@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=INITIAL_BACKOFF, max=10),
    retry=retry_if_exception_type((ClientError, TimeoutError, asyncio.TimeoutError)), # Include asyncio.TimeoutError
    before_sleep=lambda retry_state: logging.warning(
        # Access args correctly within tenacity's retry_state
        f"Retrying {retry_state.fn.__name__} for URL {retry_state.args[1]} due to {retry_state.outcome.exception()}, attempt {retry_state.attempt_number}"
    )
)
async def fetch_page(session: ClientSession, url: str) -> str | None:
    """Fetches HTML content for a given URL with retries and error handling."""
    await asyncio.sleep(DELAY_PER_REQUEST) # Add delay before each request attempt
    try:
        # Use a timeout object for more control
        timeout = aiohttp.ClientTimeout(total=30) # 30 seconds total timeout
        async with session.get(url, headers=HEADERS, timeout=timeout) as response:
            if response.status == 429:
                retry_after = int(response.headers.get("Retry-After", "5")) # Respect Retry-After header if present
                logging.warning(f"Rate limit hit (429) for {url}, waiting {retry_after} seconds")
                await asyncio.sleep(retry_after)
                # Raise exception to trigger tenacity retry after waiting
                raise ClientError(f"Rate limit hit (429) for {url}")
            # Raise an exception for 4xx/5xx errors which tenacity will catch
            response.raise_for_status()
            logging.info(f"Successfully fetched: {url} (Status: {response.status})")
            # Read response with appropriate encoding (often utf-8, but check Content-Type if needed)
            # The site uses windows-1252 according to meta tags, but requests often handles this ok.
            # Explicitly trying 'windows-1252' if utf-8 fails might be needed if errors occur.
            try:
                 # Attempt to decode using the encoding specified in headers first, fallback to utf-8/windows-1252
                 content_type = response.headers.get('Content-Type', '').lower()
                 charset = response.charset # aiohttp tries to guess
                 if charset:
                     logging.debug(f"Using detected charset {charset} for {url}")
                     return await response.text(encoding=charset, errors='ignore')
                 elif 'charset=windows-1252' in content_type:
                     logging.debug(f"Using windows-1252 based on Content-Type for {url}")
                     return await response.text(encoding='windows-1252', errors='ignore')
                 else:
                     logging.debug(f"Using default utf-8 encoding for {url}")
                     return await response.text(encoding='utf-8', errors='ignore')

            except UnicodeDecodeError:
                 logging.warning(f"Primary decode failed for {url}, trying windows-1252 fallback")
                 # Fallback encoding based on site's meta tag
                 return await response.text(encoding='windows-1252', errors='ignore')

    # Catch specific errors for logging before tenacity retries
    except ClientError as e:
        # Log status code if available
        status_code = e.status if hasattr(e, 'status') else 'N/A'
        logging.error(f"Client error fetching {url}: Status {status_code}, Message: {e}")
        raise # Re-raise for tenacity
    except asyncio.TimeoutError:
        logging.error(f"Timeout fetching {url} after 30 seconds")
        raise # Re-raise for tenacity
    except Exception as e:
        logging.exception(f"Unexpected error fetching {url}: {e}") # Log full traceback for unexpected errors
        return None # Return None for non-retryable errors

async def save_json(data, filename: str):
    """Asynchronously saves data to a JSON file."""
    try:
        async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(data, indent=2))
    except IOError as e:
        logging.error(f"Error writing JSON checkpoint {filename}: {e}")

async def load_json(filename: str) -> dict | list:
    """Asynchronously loads data from a JSON file."""
    # Determine default return type based on filename convention
    default_return = {} if filename in [CHECKPOINT_PROCESSED_STATES, PROCESSED_LIBS_CHECKPOINT] else []
    if os.path.exists(filename):
        try:
            async with aiofiles.open(filename, 'r', encoding='utf-8') as f:
                content = await f.read()
                if not content: # Handle empty file case
                    logging.warning(f"Checkpoint file {filename} is empty.")
                    return default_return
                return json.loads(content)
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from {filename}. Returning default.")
            # Optionally backup corrupted file here
            return default_return
        except IOError as e:
            logging.error(f"Error reading JSON checkpoint {filename}: {e}")
            return default_return
    return default_return # Default if file doesn't exist

async def append_jsonl(data: dict, filename: str):
    """Asynchronously appends a dictionary as a new line to a JSON Lines file."""
    try:
        async with aiofiles.open(filename, 'a', encoding='utf-8') as f:
            await f.write(json.dumps(data) + '\n')
    except IOError as e:
        logging.error(f"Error appending to JSONL file {filename}: {e}")

async def load_jsonl(filename: str) -> list[dict]:
    """Asynchronously loads all lines from a JSON Lines file."""
    data = []
    if os.path.exists(filename):
        try:
            async with aiofiles.open(filename, 'r', encoding='utf-8') as f:
                async for line in f:
                    line = line.strip()
                    if line: # Avoid empty lines
                        try:
                            data.append(json.loads(line))
                        except json.JSONDecodeError:
                            logging.error(f"Skipping malformed line in {filename}: {line}")
        except IOError as e:
            logging.error(f"Error reading JSONL file {filename}: {e}")
    return data

# --- UPDATED append_csv function ---
async def append_csv(data: list[dict], filename: str):
    """Asynchronously appends a list of dictionaries to a CSV file using csv.DictWriter."""
    if not data:
        return

    fieldnames = ['Name', 'State', 'Population', 'LibraryTechLink', 'Website'] # Ensure consistent order
    # Check if file exists and is non-empty to determine if header is needed
    # Use sync os.path for this check as it's quick and avoids async complexity just for check
    file_exists_and_has_content = os.path.exists(filename) and os.path.getsize(filename) > 0

    # We need a string buffer because csv.DictWriter writes strings,
    # and aiofiles expects strings or bytes.
    output_string_io = io.StringIO()
    # Quote all fields for safety, especially names or URLs that might have commas
    writer = csv.DictWriter(output_string_io, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)

    # Write header to the string buffer if needed
    if not file_exists_and_has_content:
        writer.writeheader()

    # Write data rows to the string buffer
    for row_dict in data:
        # Ensure all keys exist, provide default empty string if missing
        row_to_write = {field: str(row_dict.get(field, '')) for field in fieldnames}
        writer.writerow(row_to_write)

    # Get the string content from the buffer
    csv_content = output_string_io.getvalue()
    output_string_io.close() # Close the string buffer

    # Asynchronously append the generated string content to the actual file
    try:
        async with aiofiles.open(filename, 'a', encoding='utf-8', newline='') as f:
            await f.write(csv_content)
    except IOError as e:
        logging.error(f"Error appending to CSV file {filename}: {e}")
    except Exception as e:
        logging.exception(f"Unexpected error during CSV append to {filename}: {e}")


# --- Phase 1: Get State URLs ---
async def phase_1_get_state_urls():
    """Gets state URLs from the main page, using checkpoint if available."""
    if os.path.exists(CHECKPOINT_STATE_URLS):
        logging.info(f"Loading state URLs from checkpoint: {CHECKPOINT_STATE_URLS}")
        loaded_data = await load_json(CHECKPOINT_STATE_URLS)
        # Ensure it's a list, handle potential empty file case from load_json
        if isinstance(loaded_data, list) and loaded_data:
             return loaded_data
        else:
            logging.warning(f"Checkpoint {CHECKPOINT_STATE_URLS} was empty or invalid. Refetching.")

    logging.info("Fetching main page for state URLs")
    state_urls = []
    # Use a connector to potentially reuse connections if needed later
    # Limit connections to the same host to be polite
    conn = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=conn) as session:
        try:
            html = await fetch_page(session, MAIN_URL)
            if not html:
                logging.critical("Failed to fetch main page for states. Cannot proceed.")
                return [] # Return empty list if main page fetch fails

            soup = BeautifulSoup(html, 'html.parser')
            # --- Refined Logic ---
            # Find the image map first
            map_img = soup.find('img', {'usemap': '#usmap'})
            if not map_img:
                logging.error("Could not find the map image on the main page.")
                return []

            # Find all tables that appear *after* the map image
            tables_after_map = map_img.find_all_next('table')
            state_table = None
            state_link_pattern = re.compile(r'/libraries/public\.pl\?State=')

            # Iterate through the tables found after the map
            for table in tables_after_map:
                # Check if *this specific table* contains links matching the state pattern
                if table.find('a', href=state_link_pattern):
                    state_table = table # Found the correct table
                    logging.info("Found the state links table.")
                    break # Stop searching once the correct table is found

            if not state_table:
                 logging.error("Could not find the table containing state links after the map image.")
                 return []

            # Extract links from the identified state table
            for a in state_table.find_all('a', href=state_link_pattern):
                href = a.get('href')
                # Clean state name (remove potential non-breaking spaces)
                state_name = " ".join(a.text.split())
                if href and state_name:
                    # Ensure URL is correctly formed
                    full_url = BASE_URL + href if href.startswith('/') else href
                    state_urls.append({
                        'state': state_name,
                        'url': full_url
                    })
                else:
                    logging.warning(f"Found state link tag without href or text: {a}")

        except Exception as e:
            logging.exception(f"Error during Phase 1 processing: {e}")
            return [] # Return empty on error

    if state_urls:
        await save_json(state_urls, CHECKPOINT_STATE_URLS)
        logging.info(f"Saved {len(state_urls)} state URLs to {CHECKPOINT_STATE_URLS}")
    else:
        logging.warning("No state URLs were extracted.")
    return state_urls


# --- Phase 2: Get Library URLs (Per State) ---
async def fetch_state_library_urls(session: ClientSession, state_data: dict, semaphore: asyncio.Semaphore):
    """Fetches and parses library URLs for a single state."""
    state_name = state_data.get('state', 'Unknown State')
    state_url = state_data.get('url', '')
    if not state_url:
        logging.warning(f"Missing URL for state: {state_name}")
        return state_name, []

    library_urls_for_state = []
    async with semaphore: # Limit concurrency
        try:
            logging.info(f"Fetching state page: {state_name} ({state_url})")
            html = await fetch_page(session, state_url)
            if not html:
                # fetch_page logs the error, just return empty
                return state_name, []

            soup = BeautifulSoup(html, 'html.parser')
            # --- Selector Verified on AL, WA, CA ---
            # This logic assumes library links are in <p> tags following the search div
            search_div = soup.find('div', id='simplesearch')
            start_node = search_div # Default start node

            if not search_div:
                # Fallback: Sometimes the structure might slightly differ, try finding the first <hr> after header
                header = soup.find('header')
                # Find the first <hr> that is *not* inside the header itself
                first_hr = header.find_next('hr') if header else None
                # If still no good start node, default to body but log warning
                start_node = first_hr if first_hr else soup.body
                logging.warning(f"Could not find search div on state page: {state_name}. Using fallback search area starting after first <hr> or body.")

            # Find subsequent <p> tags from the start_node
            for p_tag in start_node.find_all_next('p'):
                 # Basic check to avoid footer links, might need refinement
                 if p_tag.find_parent('div', id='pagefooter'):
                     continue

                 # Find the first link within the paragraph that matches the library pattern
                 # Ensure the link is a direct child or grandchild (avoids unrelated links)
                 # Look for <a> inside <strong> first, then directly inside <p>
                 link_tag = None
                 # Check for <strong> tag containing the link
                 strong_tag = p_tag.find('strong', recursive=False)
                 if strong_tag:
                     link_tag = strong_tag.find('a', href=re.compile(r'^/library/\d+'), recursive=False)

                 # If not found in <strong>, check directly within <p>
                 if not link_tag:
                     link_tag = p_tag.find('a', href=re.compile(r'^/library/\d+'), recursive=False)


                 if link_tag:
                    href = link_tag.get('href')
                    if href:
                         full_url = BASE_URL + href if href.startswith('/') else href
                         library_urls_for_state.append({
                            'state': state_name,
                            'url': full_url
                         })
                    else:
                         logging.warning(f"Found library link tag without href in state {state_name}: {link_tag}")

            logging.info(f"Found {len(library_urls_for_state)} potential library URLs for {state_name}")
            return state_name, library_urls_for_state

        except Exception as e:
            # Log specific state and URL where error occurred
            logging.exception(f"Failed to process state {state_name} ({state_url}): {e}")
            return state_name, [] # Return empty list on error for this state

async def phase_2_get_library_urls(state_urls: list[dict]):
    """Gets library URLs for all unprocessed states."""
    logging.info("Starting Phase 2: Get Library URLs")
    processed_states_data = await load_json(CHECKPOINT_PROCESSED_STATES)
    processed_states_set = set(processed_states_data.get('states', []))

    # Load existing library URLs from JSON Lines file
    library_urls_all = await load_jsonl(CHECKPOINT_LIBRARY_URLS)
    processed_library_urls_set = {lib['url'] for lib in library_urls_all} # Keep track of URLs already saved
    logging.info(f"Loaded {len(library_urls_all)} library URLs from checkpoint {CHECKPOINT_LIBRARY_URLS}")

    unprocessed_states = [s for s in state_urls if s.get('state') not in processed_states_set]

    if not unprocessed_states:
        logging.info("All states previously processed. Returning library URLs from checkpoint.")
        return library_urls_all # Return previously collected URLs

    logging.info(f"Processing {len(unprocessed_states)} new states.")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    # Configure connector with appropriate limits
    conn = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS * 2, limit_per_host=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [fetch_state_library_urls(session, state_data, semaphore) for state_data in unprocessed_states]

        newly_found_urls_count = 0
        # Use tqdm for progress bar
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing states", unit="state"):
            try:
                state, urls_from_state = await future
                if urls_from_state is None: # Handle case where fetch_state_library_urls had critical failure
                    # Attempt to get state name if possible for logging
                    state_name_for_log = "Unknown"
                    # Check if future has task reference (may vary across Python versions)
                    task = getattr(future, '_source_traceback', [None])[-1] # Example heuristic
                    if task and hasattr(task, 'f_locals'):
                         state_name_for_log = task.f_locals.get('state_data', {}).get('state', 'Unknown')
                    logging.error(f"Task for state {state_name_for_log} failed critically.")
                    continue

                if urls_from_state: # Only process if urls were successfully fetched
                    # Append new, unique URLs to the JSONL file
                    unique_new_urls = 0
                    for lib_url_data in urls_from_state:
                        lib_url = lib_url_data.get('url')
                        if lib_url and lib_url not in processed_library_urls_set:
                            await append_jsonl(lib_url_data, CHECKPOINT_LIBRARY_URLS)
                            library_urls_all.append(lib_url_data) # Add to in-memory list
                            processed_library_urls_set.add(lib_url) # Add to set
                            unique_new_urls += 1
                    if unique_new_urls > 0:
                        logging.info(f"Appended {unique_new_urls} new library URLs for state {state} to {CHECKPOINT_LIBRARY_URLS}")
                        newly_found_urls_count += unique_new_urls

                    # Update processed states checkpoint *after* successfully processing and saving URLs
                    processed_states_set.add(state)
                    await save_json({'states': sorted(list(processed_states_set))}, CHECKPOINT_PROCESSED_STATES)
                else:
                    # Log states that yielded no URLs (could be normal or a parsing issue)
                    logging.info(f"No new library URLs found or processed for state: {state}")
                    # Mark state as processed even if no URLs found, to avoid retrying it indefinitely
                    processed_states_set.add(state)
                    await save_json({'states': sorted(list(processed_states_set))}, CHECKPOINT_PROCESSED_STATES)


            except Exception as e:
                 # Catch errors from await future itself if a task failed unexpectedly
                 logging.exception(f"Error processing a state task result: {e}")


    logging.info(f"Phase 2 finished. Found {newly_found_urls_count} new library URLs. Total unique URLs: {len(library_urls_all)}")
    return library_urls_all


# --- Phase 3: Extract Library Data ---
# --- MODIFIED fetch_library_data ---
async def fetch_library_data(session: ClientSession, library: dict, semaphore: asyncio.Semaphore):
    """Fetches and parses data for a single library, returning a tuple (data_dict_or_None, library_url)."""
    library_url = library.get('url', '')
    state_name = library.get('state', 'Unknown')
    if not library_url:
        logging.warning("Received library data without URL.")
        return None, None # Return URL as None too

    async with semaphore: # Limit concurrency
        try:
            html = await fetch_page(session, library_url)
            if not html:
                return None, library_url # Return URL even if fetch failed, to mark as processed

            soup = BeautifulSoup(html, 'html.parser')
            name = 'Name Not Found'
            population = 0
            website = 'Website Not Found'

            # Extract name (Verified based on Abbeville example)
            name_tag = soup.find('h2') # Often contains the main library name
            if name_tag:
                legal_name_span = name_tag.find('span', itemprop='legalName')
                if legal_name_span:
                    name = " ".join(legal_name_span.text.split())
                else:
                    name = " ".join(name_tag.text.split())
            else:
                logging.warning(f"Could not find h2 name tag for {library_url}")

            # Extract population (Verified based on Abbeville example structure)
            description_span = soup.find('span', itemprop='description')
            if description_span:
                population_text = " ".join(description_span.get_text(separator=' ', strip=True).split())
                match = re.search(r'serves a population of\s*(?:<strong>)?([\d,]+)(?:</strong>)?', population_text, re.I)
                if match:
                    try:
                        population = int(match.group(1).replace(',', ''))
                    except ValueError:
                        logging.warning(f"Could not parse population number '{match.group(1)}' for {library_url}")
                        population = 0
                else:
                     logging.debug(f"Population text found but pattern not matched for {library_url}. Text: '{population_text[:100]}...'")
            else:
                 logging.debug(f"Description span with itemprop='description' not found for {library_url}")

            # Extract website (Verified based on Abbeville example structure)
            main_content_div = soup.find('div', id='maincontencolumn')
            search_area = main_content_div if main_content_div else soup.body
            details_div = soup.find('div', class_='librarydetails')
            website_link = None
            if details_div:
                 website_link = details_div.find('a', string=re.compile(r'Library Web Site|Website|Homepage', re.I))
            if not website_link and search_area:
                 website_link = search_area.find('a', string=re.compile(r'Library Web Site|Website|Homepage', re.I))

            if website_link:
                 href = website_link.get('href')
                 if href and (href.startswith('http://') or href.startswith('https://')):
                     website = href
                 else:
                     logging.debug(f"Found website link tag but invalid/relative href '{href}' for {library_url}")
            else:
                 logging.debug(f"Website link tag not found for {library_url}")

            # Filter by population
            if population >= TARGET_POPULATION:
                logging.info(f"Criteria met: {name} (Pop: {population}) in {state_name}")
                data_to_return = {
                    'Name': name,
                    'State': state_name,
                    'Population': population,
                    'LibraryTechLink': library_url,
                    'Website': website
                }
                return data_to_return, library_url # Return data and URL
            else:
                if population > 0:
                     logging.info(f"Skipping {name} (Pop: {population}) - Below target {TARGET_POPULATION}")
                return None, library_url # Return None for data, but URL for processing tracking

        except Exception as e:
            logging.exception(f"Failed to process library {library_url}: {e}")
            return None, library_url # Return URL even on exception

# --- MODIFIED phase_3_extract_data ---
async def phase_3_extract_data(library_urls: list[dict]):
    """Extracts data from library pages, saving in batches and tracking processed."""
    logging.info(f"Starting Phase 3: Extract Library Data for potentially {len(library_urls)} URLs")

    processed_libs_data = await load_json(PROCESSED_LIBS_CHECKPOINT)
    processed_urls_set = set(processed_libs_data.get('processed_urls', []))
    logging.info(f"Loaded {len(processed_urls_set)} previously processed library URLs from {PROCESSED_LIBS_CHECKPOINT}.")

    urls_to_process = [lib for lib in library_urls if lib.get('url') and lib.get('url') not in processed_urls_set]
    logging.info(f"Processing {len(urls_to_process)} new/remaining library URLs.")

    if not urls_to_process:
        logging.info("No new library URLs to process in Phase 3.")
        return

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    conn = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS * 2, limit_per_host=MAX_CONCURRENT_REQUESTS)
    results_batch = []
    batch_size = 100
    processed_count_since_last_save = 0
    processed_checkpoint_save_interval = batch_size * 10

    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [fetch_library_data(session, library, semaphore) for library in urls_to_process]

        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing libraries", unit="library"):
            processed_url_to_mark = None # Initialize here
            try:
                # Await the future to get the tuple (data, url)
                data, processed_url_to_mark = await future

                # --- Processing Logic ---
                if processed_url_to_mark: # Ensure we always have the URL if the task finished
                    processed_urls_set.add(processed_url_to_mark)
                    processed_count_since_last_save += 1

                    if data: # Data means criteria met and processing succeeded
                        results_batch.append(data)

                        # Write batch to CSV when full
                        if len(results_batch) >= batch_size:
                            await append_csv(results_batch, CHECKPOINT_OUTPUT)
                            logging.info(f"Appended batch of {len(results_batch)} libraries to {CHECKPOINT_OUTPUT}")
                            results_batch = [] # Reset batch

                            # Save the set of processed URLs periodically after successful batch write
                            if processed_count_since_last_save >= processed_checkpoint_save_interval:
                                await save_json({'processed_urls': sorted(list(processed_urls_set))}, PROCESSED_LIBS_CHECKPOINT)
                                logging.info(f"Saved processed URLs checkpoint ({len(processed_urls_set)} total)")
                                processed_count_since_last_save = 0 # Reset counter

                    # Save processed URLs checkpoint less frequently if no data was returned
                    elif processed_count_since_last_save >= processed_checkpoint_save_interval:
                            await save_json({'processed_urls': sorted(list(processed_urls_set))}, PROCESSED_LIBS_CHECKPOINT)
                            logging.info(f"Saved processed URLs checkpoint ({len(processed_urls_set)} total)")
                            processed_count_since_last_save = 0
                else:
                     logging.warning("Task completed but did not return a URL to mark as processed.")


            except Exception as e:
                 # Catch errors from await future itself if a task failed unexpectedly
                 # Try to get URL from task context if possible (less reliable)
                 url_for_log = "Unknown"
                 if hasattr(future.get_coro(), 'cr_frame') and future.get_coro().cr_frame:
                    try:
                        url_for_log = future.get_coro().cr_frame.f_locals.get('library', {}).get('url', 'Unknown')
                    except Exception: pass # Ignore if frame access fails

                 logging.exception(f"Error awaiting or processing future for URL {url_for_log}: {e}")
                 # Ensure URL is marked processed even if task await fails, if we know the URL
                 if url_for_log != "Unknown":
                      processed_urls_set.add(url_for_log)
                      processed_count_since_last_save += 1


        # Append any remaining results after the loop
        if results_batch:
            await append_csv(results_batch, CHECKPOINT_OUTPUT)
            logging.info(f"Appended final batch of {len(results_batch)} libraries to {CHECKPOINT_OUTPUT}")

        # Final save of processed URLs list
        await save_json({'processed_urls': sorted(list(processed_urls_set))}, PROCESSED_LIBS_CHECKPOINT)
        logging.info(f"Final save of processed URLs checkpoint ({len(processed_urls_set)} total).")

    logging.info("Finished Phase 3: Extract Library Data.")


# --- Phase 4: Finalize Output ---
async def phase_4_finalize():
    """Renames the temporary output file to the final name."""
    logging.info("Starting Phase 4: Finalize Output")
    if os.path.exists(CHECKPOINT_OUTPUT):
        try:
            # Check if FINAL_OUTPUT already exists, potentially remove or backup
            if os.path.exists(FINAL_OUTPUT):
                 logging.warning(f"Final output file {FINAL_OUTPUT} already exists. Overwriting.")
                 os.remove(FINAL_OUTPUT)
            os.rename(CHECKPOINT_OUTPUT, FINAL_OUTPUT)
            logging.info(f"Successfully renamed {CHECKPOINT_OUTPUT} to {FINAL_OUTPUT}")

            # --- Optional Cleanup ---
            # Set to True to remove checkpoint files after successful completion
            CLEANUP_CHECKPOINTS = True
            if CLEANUP_CHECKPOINTS:
                cleanup_files = [CHECKPOINT_STATE_URLS, CHECKPOINT_PROCESSED_STATES, CHECKPOINT_LIBRARY_URLS, PROCESSED_LIBS_CHECKPOINT]
                logging.info("Cleaning up checkpoint files...")
                for f in cleanup_files:
                     if os.path.exists(f):
                          try:
                               os.remove(f)
                               logging.info(f"Removed checkpoint file: {f}")
                          except OSError as e_rem:
                               logging.error(f"Error removing checkpoint file {f}: {e_rem}")
            # ------------------------

        except OSError as e:
            logging.error(f"Error renaming output file {CHECKPOINT_OUTPUT} to {FINAL_OUTPUT}: {e}")
    else:
        # It's possible no libraries met the criteria, so the temp file might not exist.
        logging.warning(f"Temporary output file {CHECKPOINT_OUTPUT} not found. Was any data collected and met criteria?")
        # Create an empty file with header if no temp file exists but processing seemed complete
        if not os.path.exists(FINAL_OUTPUT):
             logging.info(f"Creating empty final output file {FINAL_OUTPUT} with headers.")
             try:
                  with open(FINAL_OUTPUT, 'w', newline='', encoding='utf-8') as f:
                       fieldnames = ['Name', 'State', 'Population', 'LibraryTechLink', 'Website']
                       writer = csv.writer(f)
                       writer.writerow(fieldnames)
             except IOError as e:
                  logging.error(f"Could not create empty final output file {FINAL_OUTPUT}: {e}")


# --- Main Execution ---
async def main():
    """Main function to orchestrate the scraping phases."""
    start_time = time.time()
    logging.info("--- Starting Library Scraper ---")

    try:
        # Phase 1
        state_urls = await phase_1_get_state_urls()
        if not state_urls:
            logging.error("Stopping scraper because state URLs could not be obtained.")
            return

        # Phase 2
        library_urls = await phase_2_get_library_urls(state_urls)
        if not library_urls:
            logging.warning("No library URLs found or loaded from Phase 2. Check logs and checkpoints.")
            # If library_urls is empty, Phase 3 won't run, Phase 4 will report no temp file.

        # Phase 3
        # Only run if there are URLs to process (either newly found or from checkpoint)
        if library_urls:
             await phase_3_extract_data(library_urls)
        else:
             logging.info("Skipping Phase 3 as no library URLs are available.")


        # Phase 4
        await phase_4_finalize()

    except Exception as e:
        logging.exception("An unexpected error occurred during the main execution:") # Log full traceback
    finally:
        end_time = time.time()
        logging.info(f"--- Library Scraper finished in {end_time - start_time:.2f} seconds ---")


if __name__ == '__main__':
    # Policy needed for Windows Selector event loop with aiohttp/asyncio
    if os.name == 'nt':
       # This might be needed on Windows, but can cause issues on other OSes
       # Only set it if you encounter specific loop errors on Windows.
       # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
       pass # Usually not needed for modern Python/aiohttp
    asyncio.run(main())
