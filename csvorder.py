import csv
import os
import logging
import operator # For sorting dictionaries
from fpdf import FPDF # Import the PDF library

# --- Configuration ---
INPUT_CSV_FILE = 'libraries_over_60k.csv'
OUTPUT_PDF_FILE = 'library_list_ordered_by_usage.pdf' # Output PDF filename

# !!! IMPORTANT: Ranking based on Total Checkouts FY2022 from user-provided research !!!
# Lower rank number = higher total checkouts.
STATE_USAGE_RANKING = {
    'California': 1,
    'Ohio': 2,
    'Texas': 3,
    'Florida': 4,
    'New York': 5,
    'Illinois': 6,
    'Washington': 7,
    'Michigan': 8,
    'Colorado': 9,
    'Pennsylvania': 10,
    'Massachusetts': 11,
    'Virginia': 12,
    'Indiana': 13,
    'Minnesota': 14,
    'Oregon': 15,
    'Missouri': 16,
    'Arizona': 17,
    'Maryland': 18,
    'Wisconsin': 19,
    'North Carolina': 20,
    'New Jersey': 21,
    'Utah': 22,
    'Georgia': 23,
    'Tennessee': 24,
    'Oklahoma': 25,
    'Connecticut': 26,
    'South Carolina': 27,
    'Kentucky': 28,
    'Louisiana': 29,
    'Kansas': 30,
    'Iowa': 31,
    'Alabama': 32,
    'Arkansas': 33,
    'Idaho': 34,
    'Nebraska': 35,
    'New Hampshire': 36,
    'New Mexico': 37,
    'Rhode Island': 38,
    'Montana': 39,
    'South Dakota': 40,
    'West Virginia': 41,
    'Hawaii': 42,
    'Mississippi': 43,
    'Wyoming': 44,
    'Delaware': 45,
    'Alaska': 46,
    'Vermont': 47,
    'North Dakota': 48,
    # States missing from user data ranked last
    'Maine': 49,
    'Nevada': 50,
    'District of Columbia': 51,
}


# Setup basic logging for this script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PDF(FPDF):
    """Custom PDF class to handle header/footer if needed (optional)"""
    def header(self):
        # Optional: Add header content here if desired
        pass

    def footer(self):
        # Optional: Add footer content like page numbers
        self.set_y(-15) # Position 1.5 cm from bottom
        self.set_font('Helvetica', 'I', 8)
        # Use alias_nb_pages for total page count
        self.cell(0, 10, f'Page {self.page_no()}/{{nb}}', 0, 0, 'C')

def generate_ordered_pdf(input_csv, output_pdf, state_ranking):
    """
    Reads library data from CSV, sorts by state usage rank, and writes to PDF.

    Args:
        input_csv (str): Path to the input CSV file.
        output_pdf (str): Path to the output PDF file.
        state_ranking (dict): Dictionary mapping state names (lowercase) to usage rank.
    """
    logging.info(f"Starting conversion and ordering from {input_csv} to {output_pdf}")

    # Normalize state keys in ranking for case-insensitive lookup
    state_ranking_lower = {k.lower(): v for k, v in state_ranking.items()}

    # Check if input file exists
    if not os.path.exists(input_csv):
        logging.error(f"Input file not found: {input_csv}")
        print(f"Error: Input file '{input_csv}' not found. Did the scraper run successfully and create the final CSV?")
        return

    libraries = []
    population_col_name = 'Population' # Default
    try:
        with open(input_csv, 'r', encoding='utf-8', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            # Check if required columns exist (adjust if your CSV headers differ slightly)
            if not reader.fieldnames:
                 logging.error(f"CSV file {input_csv} appears to be empty or has no header.")
                 print(f"Error: CSV file '{input_csv}' is empty or missing headers.")
                 return

            # Detect actual population column name
            if 'Population Served' in reader.fieldnames:
                 population_col_name = 'Population Served'
            elif 'Population' not in reader.fieldnames:
                 logging.error(f"CSV file {input_csv} is missing a 'Population' or 'Population Served' column. Found: {reader.fieldnames}")
                 print(f"Error: CSV file '{input_csv}' is missing a Population column.")
                 return

            required_columns = ['Name', 'State', population_col_name, 'Website', 'LibraryTechLink']
            if not all(col in reader.fieldnames for col in required_columns):
                 logging.error(f"CSV file {input_csv} is missing one or more required columns: {required_columns}. Found: {reader.fieldnames}")
                 print(f"Error: CSV file '{input_csv}' is missing required columns (needs: {', '.join(required_columns)}).")
                 return

            # Read data and add rank
            unknown_states = set()
            for row in reader:
                state_name = row.get('State', '').strip()
                # Use case-insensitive lookup
                rank = state_ranking_lower.get(state_name.lower(), 999) # Default to a high rank if state not found
                if rank == 999 and state_name:
                    unknown_states.add(state_name)

                # Store population using the detected column name, keep original key for sorting if needed
                row['Population_Value'] = row.get(population_col_name, 'N/A')
                row['UsageRank'] = rank # Add the rank to the dictionary
                libraries.append(row)

            if unknown_states:
                logging.warning(f"Could not find ranking for the following states (assigned rank 999): {', '.join(sorted(list(unknown_states)))}")

    except FileNotFoundError:
        logging.error(f"Input file not found during processing: {input_csv}")
        print(f"Error: Input file '{input_csv}' not found.")
        return
    except Exception as e:
        logging.exception(f"An unexpected error occurred reading {input_csv}: {e}")
        print(f"An unexpected error occurred reading the CSV: {e}")
        return

    if not libraries:
        logging.warning(f"No libraries read from {input_csv}. Cannot generate output.")
        print("No library data found in the CSV file.")
        return

    # Sort libraries primarily by UsageRank (ascending), then by State name, then by Library Name
    libraries.sort(key=operator.itemgetter('UsageRank', 'State', 'Name'))
    logging.info(f"Sorted {len(libraries)} libraries based on state usage rank.")

    # --- Generate PDF ---
    try:
        pdf = PDF(orientation='P', unit='mm', format='A4') # Use custom class for footer
        pdf.alias_nb_pages() # Enable total page count in footer
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=15) # Enable auto page breaks

        # --- Title ---
        pdf.set_font('Helvetica', 'B', 16)
        pdf.cell(0, 10, "Libraries Serving 60,000+ Population", ln=1, align='C')
        pdf.set_font('Helvetica', 'I', 10)
        pdf.cell(0, 8, "(Ordered by State Total Checkouts FY2022 - Rank 1 = Highest)", ln=1, align='C')
        pdf.ln(8) # Add space after title

        current_rank = -1
        line_height = 6 # Adjust line height for better spacing

        for library in libraries:
            rank = library.get('UsageRank', 999)
            name = library.get('Name', 'N/A')
            state = library.get('State', 'N/A')
            population = library.get('Population_Value', 'N/A') # Use the value read earlier
            website = library.get('Website', 'Not Found')
            # lib_tech_link = library.get('LibraryTechLink', 'N/A') # Uncomment if needed

            # --- Page Break Check & Rank Header ---
            entry_height_estimate = (line_height * 4) + 6 # Estimate height: 4 lines + spacing
            if pdf.get_y() + entry_height_estimate > pdf.page_break_trigger and not pdf.in_footer:
                 pdf.add_page()
                 current_rank = -1 # Reset rank header check for new page

            rank_display = str(rank) if rank != 999 else "Unknown"
            if rank != current_rank:
                if current_rank != -1: # Add extra space between rank groups
                    pdf.ln(line_height * 1.5) # More space between groups
                pdf.set_font('Helvetica', 'B', 12)
                pdf.cell(0, line_height * 1.5, f"--- State Usage Rank: {rank_display} ---", ln=1, align='L')
                pdf.set_font('Helvetica', '', 10) # Reset font
                current_rank = rank
                pdf.ln(line_height / 2) # Space after rank header

            # --- Library Details ---
            # Use Write for better flow control and link handling
            pdf.set_font('Helvetica', 'B', 10)
            pdf.write(line_height, "Name: ")
            pdf.set_font('Helvetica', '', 10)
            # Use multi_cell for name if it might wrap, otherwise write is fine
            # pdf.multi_cell(0, line_height, name) # If wrapping is needed
            pdf.write(line_height, name)
            pdf.ln(line_height) # New line

            pdf.set_font('Helvetica', 'B', 10)
            pdf.write(line_height, "State: ")
            pdf.set_font('Helvetica', '', 10)
            pdf.write(line_height, state)
            pdf.ln(line_height)

            pdf.set_font('Helvetica', 'B', 10)
            pdf.write(line_height, "Population Served: ")
            pdf.set_font('Helvetica', '', 10)
            pdf.write(line_height, str(population)) # Ensure population is string
            pdf.ln(line_height)

            # Website Link (using write for better link handling)
            pdf.set_font('Helvetica', 'B', 10)
            pdf.write(line_height, "Website: ")
            pdf.set_font('Helvetica', '', 10)
            if website and website != 'Not Found' and website.startswith('http'):
                pdf.set_text_color(0, 0, 255) # Blue
                pdf.set_font('', 'U') # Underline
                pdf.write(line_height, website, link=website)
                pdf.set_font('Helvetica', '', 10) # Reset font
                pdf.set_text_color(0, 0, 0) # Reset color
            else:
                pdf.write(line_height, website) # Write "Not Found" or invalid URL as plain text
            pdf.ln(line_height * 2) # Add extra space after each library entry


        # Save the PDF
        pdf.output(output_pdf)
        logging.info(f"Successfully wrote ordered PDF to {output_pdf}")
        print(f"Successfully created ordered PDF: {output_pdf}")

    except Exception as e:
        logging.exception(f"An unexpected error occurred generating {output_pdf}: {e}")
        print(f"An unexpected error occurred generating the PDF: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    generate_ordered_pdf(INPUT_CSV_FILE, OUTPUT_PDF_FILE, STATE_USAGE_RANKING)
