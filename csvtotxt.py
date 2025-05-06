import csv
import os
import logging

# --- Configuration ---
INPUT_CSV_FILE = 'libraries_over_60k.csv'
OUTPUT_TXT_FILE = 'library_list.txt'

# Setup basic logging for this script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def convert_csv_to_txt(input_csv, output_txt):
    """
    Reads library data from a CSV file and writes it to a formatted text file.

    Args:
        input_csv (str): Path to the input CSV file.
        output_txt (str): Path to the output TXT file.
    """
    logging.info(f"Starting conversion from {input_csv} to {output_txt}")

    # Check if input file exists
    if not os.path.exists(input_csv):
        logging.error(f"Input file not found: {input_csv}")
        print(f"Error: Input file '{input_csv}' not found. Did the scraper run successfully?")
        return

    try:
        # Open the output file first to ensure we can write to it
        with open(output_txt, 'w', encoding='utf-8') as txtfile:
            # Open the input CSV file
            with open(input_csv, 'r', encoding='utf-8', newline='') as csvfile:
                # Use DictReader to easily access columns by name
                reader = csv.DictReader(csvfile)

                # Check if required columns exist
                required_columns = ['Name', 'State', 'Population', 'Website']
                if not all(col in reader.fieldnames for col in required_columns):
                    logging.error(f"CSV file {input_csv} is missing one or more required columns: {required_columns}")
                    print(f"Error: CSV file '{input_csv}' is missing required columns (Name, State, Population, Website).")
                    return

                count = 0
                # Write header for the text file
                txtfile.write("Libraries Serving 60,000+ Population\n")
                txtfile.write("======================================\n\n")

                # Process each row
                for row in reader:
                    name = row.get('Name', 'N/A')
                    state = row.get('State', 'N/A')
                    population = row.get('Population', 'N/A')
                    website = row.get('Website', 'Not Found') # Get website URL

                    # Format the output for the text file
                    txtfile.write(f"Name: {name}\n")
                    txtfile.write(f"State: {state}\n")
                    txtfile.write(f"Population Served: {population}\n")
                    # Write the website URL directly - email clients usually linkify it
                    txtfile.write(f"Website: {website}\n")
                    txtfile.write("-" * 30 + "\n\n") # Separator
                    count += 1

            logging.info(f"Successfully processed {count} libraries.")
            print(f"Successfully converted {count} libraries to {output_txt}")

    except FileNotFoundError:
        # This case is handled above, but keep for safety
        logging.error(f"Input file not found during processing: {input_csv}")
        print(f"Error: Input file '{input_csv}' not found.")
    except KeyError as e:
         logging.error(f"Missing expected column in CSV: {e}")
         print(f"Error: Missing column '{e}' in {input_csv}. Please check the CSV format.")
    except IOError as e:
        logging.error(f"Error writing to output file {output_txt}: {e}")
        print(f"Error: Could not write to output file '{output_txt}'. Check permissions.")
    except Exception as e:
        logging.exception(f"An unexpected error occurred during conversion: {e}")
        print(f"An unexpected error occurred: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    convert_csv_to_txt(INPUT_CSV_FILE, OUTPUT_TXT_FILE)
