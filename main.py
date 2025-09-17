from mcp.server.fastmcp import FastMCP
import requests
from bs4 import BeautifulSoup
import json

import logging
import os
from datetime import datetime

# Setup logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_filename = f"mcp_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_filepath = os.path.join(log_dir, log_filename)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        # logging.FileHandler(log_filepath),
        logging.StreamHandler()
    ],
)

logger = logging.getLogger(__name__)

mcp = FastMCP("padma-mssql")


def run_raw_mssql_query_via_adminer(query):
    """
    Run MSSQL query on Padma MSSQL database via Adminer and extract results from HTML response.

    Args:
        query (str): Raw SQL query to execute

    Returns:
        str: Extracted result content from div id "content"
    """
    host = os.getenv("adminer_host")
    url = host + "/"

    cookies = {
        "adminer_key": os.getenv("adminer_key"),
        "adminer_version": os.getenv("adminer_version"),
        "adminer_sid": os.getenv("adminer_sid"),
        "adminer_permanent": os.getenv("adminer_permanent"),
    }

    params = {
        "mssql": os.getenv("mssql_host"),
        "username": os.getenv("mssql_username"),
        "db": os.getenv("mssql_db"),
        "ns": os.getenv("mssql_ns"),
        "sql": query,
    }

    referrer = f"{url}?mssql={os.getenv('mssql_host')}&username={os.getenv('mssql_username')}&db={os.getenv('mssql_db')}&ns={os.getenv('mssql_ns')}&sql="

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Origin": host,
        "Referer": referrer,
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    }

    try:
        # First get the page to get the CSRF token
        response = requests.get(
            url,
            params=params,
            headers=headers,
            cookies=cookies,
            verify=False,
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # Extract CSRF token from the form
        csrf_token = None
        token_input = soup.find("input", {"name": "token"})
        if token_input:
            csrf_token = token_input.get("value")

        if not csrf_token:
            return "Error: Could not extract CSRF token from page"

        # Now submit the form with the extracted token
        form_data = {"query": query, "limit": "", "token": csrf_token}

        response = requests.post(
            url,
            params=params,
            headers=headers,
            cookies=cookies,
            data=form_data,
            verify=False,
        )
        response.raise_for_status()

        logger.info("Response status: %s", response.status_code)
        logger.info("Response URL: %s", response.url)

        soup = BeautifulSoup(response.text, "html.parser")

        # Debug: Look for specific table structures
        logger.debug("Looking for tables with specific classes or IDs...")
        result_tables = soup.find_all("table", class_="table")
        logger.debug("Found %d tables with class 'table'", len(result_tables))

        # Also look for tables within specific divs
        content_div = soup.find("div", id="content")
        if content_div:
            content_tables = content_div.find_all("table")
            logger.debug("Found %d tables within content div", len(content_tables))

        # Look for table results
        tables = soup.find_all("table")
        logger.debug("Found %d total tables", len(tables))

        if content_tables:
            result_data = []
            for table_idx, table in enumerate(content_tables):
                logger.debug("Processing table %d", table_idx + 1)

                # Extract headers from thead > tr > th (proper table structure)
                headers = []
                thead = table.find("thead")
                if thead:
                    header_row = thead.find("tr")
                    if header_row:
                        header_cells = header_row.find_all("th")
                        logger.debug("  Raw header cells found: %d", len(header_cells))

                        # The th tags are unclosed, so we need to extract individual column names
                        # from the concatenated text by looking for the actual column boundaries
                        for i, cell in enumerate(header_cells):
                            header_text = cell.get_text(strip=True)
                            logger.debug("    Header cell %d: '%s'", i, header_text)

                            if header_text:
                                # Instead of trying to parse malformed headers, let's work backwards
                                # from the data to understand the column structure

                                # For now, just add the raw header text - we'll fix the column structure later
                                headers.append(header_text)
                                logger.debug("      Added raw header: %s", header_text)

                # Fallback to first row if no thead found
                if not headers:
                    rows = table.find_all("tr")
                    if rows:
                        header_row = rows[0]
                        header_cells = header_row.find_all(["th", "td"])
                        for cell in header_cells:
                            headers.append(cell.get_text(strip=False))

                logger.debug("  Raw headers: %s", headers)

                # First, try to extract concatenated column names
                # by progressively removing the next column name from the current one
                if len(headers) > 1:  # Only try if we have multiple headers
                    logger.debug("  Attempting to extract concatenated column names...")

                    extracted_headers = []
                    for i in range(len(headers)):
                        current_header = headers[i]
                        logger.debug(
                            "    Processing header %d: '%s'", i, current_header
                        )

                        if i < len(headers) - 1:
                            # Not the last header - remove the next header from the current one
                            next_header = headers[i + 1]
                            logger.debug("      Next header: '%s'", next_header)

                            # Find the position of the next header in the current header
                            if next_header in current_header:
                                # Extract the part before the next header
                                extracted_part = current_header[
                                    : current_header.find(next_header)
                                ]
                                if extracted_part:
                                    extracted_headers.append(extracted_part)
                                    logger.debug(
                                        "      Extracted: '%s' (removed '%s')",
                                        extracted_part,
                                        next_header,
                                    )
                                else:
                                    # If nothing left after removal, keep the original
                                    extracted_headers.append(current_header)
                                    logger.debug(
                                        "      Nothing left after removal, keeping: '%s'",
                                        current_header,
                                    )
                            else:
                                # Next header not found in current header, keep the original
                                extracted_headers.append(current_header)
                                logger.debug(
                                    "      Next header not found, keeping: '%s'",
                                    current_header,
                                )
                        else:
                            # Last header - no truncation needed
                            extracted_headers.append(current_header)
                            logger.debug(
                                "      Last header, no truncation: '%s'", current_header
                            )

                    if len(extracted_headers) > 1:
                        logger.debug("    Extracted headers: %s", extracted_headers)
                        # Use the extracted headers if we found meaningful ones
                        if any(len(h) >= 2 for h in extracted_headers):
                            headers = extracted_headers
                            logger.debug("    Using extracted headers: %s", headers)
                            # Skip data reconstruction since we have the actual column names
                            logger.debug(
                                "  Skipping data reconstruction - using extracted column names"
                            )
                        else:
                            logger.debug(
                                "    Extracted headers too short, keeping original ones"
                            )
                    else:
                        logger.debug(
                            "    Could not extract meaningful headers, keeping original ones"
                        )

                # Only attempt data reconstruction if we don't have extracted column names
                if len(headers) == 1 or any(len(h) < 2 for h in headers):
                    # For single headers or very short headers, we can't extract meaningful column names
                    # Just keep the headers as they are
                    logger.debug(
                        "  Single header or short headers detected, keeping as is"
                    )

                logger.debug("  Final headers: %s", headers)

                if headers:
                    # The HTML is malformed - there are unclosed tags and nested <tr> tags inside <td> tags
                    # We need to extract all <td> elements and get the first text content from each
                    all_td_elements = table.find_all("td")
                    logger.debug("  Found %d td elements", len(all_td_elements))

                    # Extract all text content from td elements
                    all_texts = []
                    for td in all_td_elements:
                        text_content = ""
                        for content in td.contents:
                            if isinstance(content, str):
                                text_content += content.strip()
                            else:
                                if getattr(content, "name", None) == "td":
                                    break
                                if getattr(content, "name", None) == "tr":
                                    break

                                text_content += str(content)

                        if text_content and text_content not in headers:
                            clean_text = " ".join(text_content.split())
                            if clean_text:
                                all_texts.append(clean_text)

                    logger.debug("  Extracted %d text elements", len(all_texts))

                    # Generic approach: Create rows based on the number of headers and data pattern
                    if len(headers) == 1:
                        # Single column case - each text becomes a row
                        for text in all_texts:
                            row_data = {headers[0]: text}
                            result_data.append(row_data)
                            logger.debug("    Added single column row: %s", row_data)

                    elif len(headers) == 2:
                        # Two column case - try to pair the data
                        # Look for alternating patterns in the data
                        values_for_col1 = []
                        values_for_col2 = []

                        for i, text in enumerate(all_texts):
                            if i % 2 == 0:  # Even indices
                                values_for_col1.append(text)
                            else:  # Odd indices
                                values_for_col2.append(text)

                        # Create rows by pairing the values
                        max_rows = min(len(values_for_col1), len(values_for_col2))
                        for i in range(max_rows):
                            row_data = {
                                headers[0]: values_for_col1[i],
                                headers[1]: values_for_col2[i],
                            }
                            result_data.append(row_data)
                            logger.debug("    Added paired row: %s", row_data)

                    elif len(headers) > 2:
                        # Multi-column case - handle any number of columns dynamically
                        logger.debug(
                            "  Multi-column case with %d headers", len(headers)
                        )

                        # Create arrays for each column
                        column_values = [[] for _ in range(len(headers))]

                        # Distribute data across columns in a round-robin fashion
                        for i, text in enumerate(all_texts):
                            column_index = i % len(headers)
                            column_values[column_index].append(text)

                        # Find the minimum length to avoid index errors
                        min_length = min(len(col) for col in column_values)

                        # Create rows by grouping values from each column
                        for row_index in range(min_length):
                            row_data = {}
                            for col_index, header in enumerate(headers):
                                if col_index < len(column_values) and row_index < len(
                                    column_values[col_index]
                                ):
                                    row_data[header] = column_values[col_index][
                                        row_index
                                    ]
                                else:
                                    row_data[header] = ""

                            result_data.append(row_data)
                            logger.debug(
                                "    Added %d-column row: %s", len(headers), row_data
                            )

                    else:
                        # Fallback case - create rows with available data
                        for i, text in enumerate(all_texts):
                            row_data = {}
                            for j, header in enumerate(headers):
                                if j == 0:
                                    row_data[header] = text
                                else:
                                    row_data[header] = ""

                            result_data.append(row_data)
                            logger.debug("    Added fallback row: %s", row_data)

                    logger.debug("  Total data rows extracted: %d", len(result_data))
                else:
                    logger.debug("  No headers found, skipping table")

            logger.debug("Total result_data items: %d", len(result_data))
            if result_data:
                return json.dumps(result_data, indent=2)
            else:
                return "No data rows found in tables"
        else:
            return "No result"

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {str(e)}", exc_info=True)
        return f"Request error: {str(e)}"
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        return f"Error: {str(e)}"


mcp.tool()(run_raw_mssql_query_via_adminer)


if __name__ == "__main__":
    mcp.run(transport="stdio")
