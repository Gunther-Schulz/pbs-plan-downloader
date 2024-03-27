import shutil
from PyPDF2 import PdfReader, errors
from PyPDF2 import PdfReader
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import urllib.parse
import tempfile
from tqdm import tqdm

# Load the CSV file
csv_file = 'fpläne.csv'
df = pd.read_csv(csv_file)

# # Filter rows where 'anzeigename' contains "solar" or "photo"
# df = df[df['anzeigename'].str.contains('solar|photo', case=False, na=False)]

# Convert 'genehmigungsdatum' to datetime
df['genehmigungsdatum'] = pd.to_datetime(
    df['genehmigungsdatum'], errors='coerce')

# Drop rows with NaT in 'genehmigungsdatum'
df = df.dropna(subset=['genehmigungsdatum'])

# Specify date range
start_date = '2021-01-01'
end_date = '2024-12-31'

# Filter the DataFrame
df = df[(df['genehmigungsdatum'] >= start_date)
        & (df['genehmigungsdatum'] <= end_date)]

# Main download directory
main_dir = os.path.splitext(csv_file)[0]
if not os.path.exists(main_dir):
    os.makedirs(main_dir)

# Iterate over the DataFrame rows
for i, row in df.iterrows():
    link = row['link']
    genehmigungsdatum = row['genehmigungsdatum']

    # Download the HTML
    response = requests.get(link)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the Gemeinde
    gemeinde_section = soup.find('th', text='Gemeindename')
    if gemeinde_section is not None:
        gemeinde_section = gemeinde_section.find_next_sibling('td')
    gemeinde = gemeinde_section.text.strip() if gemeinde_section else 'unknown'

    # Get the Nummer directly from the DataFrame
    nummer = row['nummer']

    # Convert the year to a string
    year_only = genehmigungsdatum.strftime('%Y')

    # Use the date, Gemeinde, and Nummer in the directory name
    directory = os.path.join(main_dir, f'{year_only}_{gemeinde}_{nummer}')
    os.makedirs(directory, exist_ok=True)

    # Rest of your code...

    print(f'Downloading PDFs from link {i}...', link)
    # Download the HTML
    response = requests.get(link)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the Downloads section
    downloads_section = soup.find('th', text='Downloads')
    if downloads_section is None:
        downloads_section = soup.find('th', text='Download')

    if downloads_section is not None:
        downloads_section = downloads_section.find_next_sibling('td')

    if downloads_section is not None:
        # Find all PDF links in the Downloads section
        pdf_links = downloads_section.find_all('a', href=True)

        # Download each PDF
        for pdf_link in pdf_links:
            pdf_url = urllib.parse.quote(pdf_link['href'], safe='/:')
            pdf_name = urllib.parse.unquote(
                pdf_url.split('/')[-1]).replace(' ', '_')
            pdf_path = os.path.join(directory, pdf_name)

            # Skip download if PDF already exists
            if os.path.exists(pdf_path):
                print(
                    f'Skipping download for PDF {pdf_name} as it already exists.')
                continue

            # Download to a temporary file
            response = requests.get(pdf_url, stream=True)
            total_size_in_bytes = int(
                response.headers.get('content-length', 0))
            block_size = 1024  # 1 Kibibyte
            progress_bar = tqdm(total=total_size_in_bytes,
                                unit='iB', unit_scale=True)
            with tempfile.NamedTemporaryFile(delete=False) as temp:
                for data in response.iter_content(block_size):
                    progress_bar.update(len(data))
                    temp.write(data)
                progress_bar.close()
                if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
                    print("ERROR, something went wrong")
                temp_pdf_path = temp.name

            # Rename the temporary file to the actual name
            os.rename(temp_pdf_path, pdf_path)


# Create a text file to store the PDFs that contain "solar" or "photov"
with open('found_pdfs.txt', 'w') as f:
    # Walk through the main directory
    for root, dirs, files in os.walk(main_dir):
        for file in files:
            if file.endswith('.pdf'):
                # Open the PDF
                pdf_path = os.path.join(root, file)
                try:
                    with open(pdf_path, 'rb') as pdf_file:
                        # Read the PDF
                        pdf_reader = PdfReader(pdf_file)
                        text = ''
                        for page in pdf_reader.pages:
                            text += page.extract_text()

                        # Search for "solar" or "photov"
                        if 'solar' in text.lower() or 'photov' in text.lower():
                            # Write the relative directory name and PDF name to the text file
                            f.write(f'{os.path.relpath(pdf_path, main_dir)}\n')

                            # Move the directory to a different directory
                            destination_dir = './solar_im_text_gefunden'
                            # create dir if not exist
                            if not os.path.exists(destination_dir):
                                os.makedirs(destination_dir)

                            # Get the name of the current directory
                            current_dir_name = os.path.basename(root)
                            # Construct the path where the directory would be if it was moved
                            new_dir_path = os.path.join(
                                destination_dir, current_dir_name)

                            # Only move the directory if it doesn't already exist in the destination
                            if not os.path.exists(new_dir_path):
                                shutil.move(root, destination_dir)

                            # Break the inner loop
                            break

                except errors.PdfReadError:
                    print(f"Could not read {pdf_path}. It may be corrupted.")
