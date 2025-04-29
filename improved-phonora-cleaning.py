import pandas as pd
import re
import uuid
import numpy as np
import asyncio
import aiofiles
import os
from collections import defaultdict
from tqdm.asyncio import tqdm as async_tqdm
from tqdm import tqdm
from rapidfuzz import process, fuzz
import json
import csv
from typing import Dict, List, Tuple, Any, Set
from concurrent.futures import ProcessPoolExecutor

class PhonoraDataCleaner:
    """
    A class to clean, standardize, and enrich French contact data with improved
    performance using rapidfuzz for fuzzy matching and async for I/O operations.
    """
    
    def __init__(self):
        # List of correct French city names with hyphens for later reference
        self.correct_cities = [
            "La Roche-sur-Yon", "Fontenay-le-Comte", "Saint-Maixent-l'École", "Chalon-sur-Saône",
            "Saint-Jean-de-Luz", "Le Puy-en-Velay", "Saint-Lô", "Sainte-Marie-du-Mont",
            "Sainte-Mère-Église", "Grandcamp-Maisy", "Saint-Brieuc", "Saint-Malo",
            "Saint-Benoît-sur-Loire", "Saint-Amand-Montrond", "Azay-le-Rideau", "Beaugency",
            "Bourmont-entre-Meuse-et-Mouzon", "Blénod-lès-Pont-à-Mousson", "Coise-Saint-Jean-Pied-Gauthier",
            "Montigny-Mornay-Villeneuve-sur-Vingeanne", "Poiseul-la-Ville-et-Laperrière",
            "Belleville-et-Châtillon-sur-Bar", "Bout-du-Pont-de-Larn", "Brey-et-Maison-du-Bois",
            "Dhuys-et-Morin-en-Brie", "Échenans-sous-Mont-Vaudois", "Flavigny-le-Grand-et-Beaurain",
            "Haut-du-Them-Château-Lambert", "Jugon-les-Lacs-Commune-Nouvelle",
            "Lacarry-Arhan-Charritte-de-Haut", "Biéville-Beuville", "Ingrandes-le-Fresne-sur-Loire",
            "La Chapelle-Fleurigné", "Source-Seine"
        ]
        
        # French city and country correction dictionaries will be loaded in setup_correction_data
        self.french_city_corrections = {}
        self.country_corrections = {}
        self.city_country_map = {}

    async def setup_correction_data(self):
        """Initialize the correction dictionaries used for cleaning"""
        print("🔧 Setting up reference data for corrections...")
        
        # Basic French city corrections - will be enhanced with fuzzy matching
        self.french_city_corrections = {
            # Major cities
            'paris': 'Paris',
            'pari': 'Paris',
            'pariz': 'Paris',
            'pariss': 'Paris',
            'marseille': 'Marseille',
            'marseilles': 'Marseille',
            'marsaille': 'Marseille',
            'lyon': 'Lyon',
            'lyons': 'Lyon',
            'lion': 'Lyon',
            'toulouse': 'Toulouse',
            'toulouze': 'Toulouse',
            'tolouse': 'Toulouse',
            'nice': 'Nice',
            'nis': 'Nice',
            'nantes': 'Nantes',
            'nante': 'Nantes',
            'strasbourg': 'Strasbourg',
            'straburg': 'Strasbourg',
            'strassbourg': 'Strasbourg',
            
            # Medium cities
            'montpellier': 'Montpellier',
            'montpelier': 'Montpellier',
            'bordeaux': 'Bordeaux',
            'bordeau': 'Bordeaux',
            'bordo': 'Bordeaux',
            'lille': 'Lille',
            'lile': 'Lille',
            'rennes': 'Rennes',
            'renne': 'Rennes',
            'reims': 'Reims',
            'reim': 'Reims',
            'st-etienne': 'Saint-Étienne',
            'saint-etienne': 'Saint-Étienne',
            'toulon': 'Toulon',
            'grenoble': 'Grenoble',
            'dijon': 'Dijon',
            'angers': 'Angers',
            'nimes': 'Nîmes',
            
            # Add more common corrections...
        }
        
        # Country corrections
        self.country_corrections = {
            'france': 'France',
            'fr': 'France',
            'francais': 'France',
            'française': 'France',
            'espagne': 'Espagne',
            'spain': 'Espagne',
            'españa': 'Espagne',
            'allemagne': 'Allemagne',
            'germany': 'Allemagne',
            'italie': 'Italie',
            'italy': 'Italie',
            'royaume-uni': 'Royaume-Uni',
            'uk': 'Royaume-Uni',
            'united kingdom': 'Royaume-Uni',
            'angleterre': 'Royaume-Uni',
            'belgique': 'Belgique',
            'belgium': 'Belgique',
            'pays-bas': 'Pays-Bas',
            'netherlands': 'Pays-Bas',
            'hollande': 'Pays-Bas',
            'suisse': 'Suisse',
            'switzerland': 'Suisse',
            
            # Add more country corrections...
        }
        
        # Generate standardized city names (normalize spacing and capitalization)
        self.name_mapping = {re.sub(r'[- ]', '', city.lower()): city for city in self.correct_cities}
        
        print("✅ Reference data setup complete")

    @staticmethod
    def clean_text(text: str) -> str:
        """
        Clean text by removing special characters while preserving French accents.
        """
        if pd.isna(text) or not isinstance(text, str):
            return text
        # Keep letters (including French accents), spaces, hyphens, and apostrophes
        text = re.sub(r'[^a-zA-ZÀ-ÿ\s\-\']', '', text)
        return text.strip()

    def fix_city_name_with_fuzzy(self, city: str, threshold: int = 85) -> str:
        """
        Fix city names using fuzzy matching with rapidfuzz.
        This provides more accurate matching than simple dictionary lookups.
        """
        try:
            if pd.isna(city) or not isinstance(city, str) or city.lower() in ('inconnu', 'n/a', ''):
                return city
                
            city = city.strip()
            city_lower = city.lower()
            
            # Direct match in our corrections dictionary
            if city_lower in self.french_city_corrections:
                return self.french_city_corrections[city_lower]
                
            # Check for normalized match (without hyphens/spaces)
            normalized = re.sub(r'[- ]', '', city_lower)
            if normalized in self.name_mapping:
                return self.name_mapping[normalized]
                
            # If no direct match, try fuzzy matching against our known correct cities
            # Get combined list of reference cities
            reference_cities = list(set(self.french_city_corrections.values()) | 
                                    set(self.name_mapping.values()))
            
            # Use rapidfuzz to find closest match
            match_result = process.extractOne(
                city, 
                reference_cities,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=threshold
            )
            
            # Check if we got a valid match
            if match_result is not None:
                match, score, _ = match_result
                if score >= threshold:
                    return match
                
            # If no good fuzzy match, return original with proper capitalization
            return city.title()
        except Exception as e:
            print(f"Error in fix_city_name_with_fuzzy: {e}")
            return city  # Return original if error occurs

    def standardize_country_with_fuzzy(self, country: str, threshold: int = 85) -> str:
        """
        Standardize country names using fuzzy matching.
        """
        try:
            if pd.isna(country) or not isinstance(country, str) or country.lower() in ('inconnu', 'n/a', ''):
                return country
                
            country = country.strip()
            country_lower = country.lower()
            
            # Direct match in our corrections dictionary
            if country_lower in self.country_corrections:
                return self.country_corrections[country_lower]
                
            # If no direct match, try fuzzy matching
            reference_countries = list(set(self.country_corrections.values()))
            
            match_result = process.extractOne(
                country, 
                reference_countries,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=threshold
            )
            
            # Check if we got a valid match
            if match_result is not None:
                match, score, _ = match_result
                if score >= threshold:
                    return match
                
            # If no good fuzzy match, return original with proper capitalization
            return country.title()
        except Exception as e:
            print(f"Error in standardize_country_with_fuzzy: {e}")
            return country  # Return original if error occurs

    async def create_city_country_map(self, input_file: str, chunk_size: int = 1000000) -> Dict[str, str]:
        """
        Create a mapping of cities to countries from the existing data.
        This helps in filling missing country information.
        """
        print("🗺️ Creating city-to-country mapping...")
        city_country_map = defaultdict(lambda: defaultdict(int))
        
        # Process the file in chunks to handle large files
        try:
            # Get column information from the first chunk
            first_chunk = pd.read_csv(input_file, nrows=1)
            columns = list(first_chunk.columns)
            
            print(f"Available columns in CSV: {columns}")
            
            # Find relevant column names - more flexible matching
            city_cols = [col for col in columns if 'city' in col.lower()]
            country_cols = [col for col in columns if 'country' in col.lower()]
            
            if not city_cols or not country_cols:
                print("⚠️ Warning: Could not find city or country columns in the CSV")
                return {}
                
            print(f"Found city columns: {city_cols}")
            print(f"Found country columns: {country_cols}")
            
            # Pair city and country columns based on naming patterns
            column_pairs = []
            for city_col in city_cols:
                # Try to match corresponding country column
                base_name = city_col.lower().replace('city', '')
                matching_country_col = next((c for c in country_cols if base_name in c.lower()), None)
                
                if matching_country_col:
                    column_pairs.append((city_col, matching_country_col))
            
            if not column_pairs:
                # If no matching pairs found, use all combinations as fallback
                column_pairs = [(city_col, country_col) for city_col in city_cols for country_col in country_cols]
            
            print(f"Using column pairs for mapping: {column_pairs}")
            
            # Process file in chunks
            with tqdm(desc="Building city-country map") as pbar:
                for chunk in pd.read_csv(input_file, chunksize=chunk_size, low_memory=False, 
                                        on_bad_lines='skip'):
                    # Process each city-country column pair
                    for city_col, country_col in column_pairs:
                        # Filter for rows with valid city and country
                        try:
                            mask = (
                                chunk[city_col].notna() & 
                                chunk[city_col].astype(str).str.lower().ne('inconnu') & 
                                chunk[country_col].notna() & 
                                chunk[country_col].astype(str).str.lower().ne('inconnu')
                            )
                            
                            cities = chunk.loc[mask, city_col].astype(str).str.lower()
                            countries = chunk.loc[mask, country_col].astype(str).str.lower()
                            
                            for city, country in zip(cities, countries):
                                if pd.notna(city) and pd.notna(country):
                                    city_country_map[city][country] += 1
                        except Exception as e:
                            print(f"Error processing columns {city_col}/{country_col}: {e}")
                    
                    pbar.update(1)
        
            # Convert the defaultdict to a regular dict with the most common country for each city
            result = {}
            for city, countries in city_country_map.items():
                if countries:
                    result[city] = max(countries.items(), key=lambda x: x[1])[0]
            
            print(f"✅ Created mapping for {len(result)} cities")
            return result
            
        except Exception as e:
            print(f"❌ Error while creating city-country map: {e}")
            return {}

    def process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Process a single DataFrame chunk - this function can be executed
        in parallel with ProcessPoolExecutor.
        """
        try:
            # Clean proper nouns
            if 'first_name' in chunk.columns:
                chunk['first_name'] = chunk['first_name'].apply(self.clean_text).str.title()
            if 'last_name' in chunk.columns:
                chunk['last_name'] = chunk['last_name'].apply(self.clean_text).str.title()
                
            # Apply city and country cleaning
            city_columns = [col for col in chunk.columns if 'city' in col.lower()]
            country_columns = [col for col in chunk.columns if 'country' in col.lower()]
            
            # Clean city values with fuzzy matching - safer approach
            for col in city_columns:
                chunk[col] = chunk[col].apply(lambda x: self.fix_city_name_with_fuzzy(x) if pd.notna(x) else x)
                
            # Clean country values with fuzzy matching - safer approach
            for col in country_columns:
                chunk[col] = chunk[col].apply(lambda x: self.standardize_country_with_fuzzy(x) if pd.notna(x) else x)
                
            # Fill missing countries based on city information if we have city_country_map
            if self.city_country_map and len(city_columns) == len(country_columns):
                for city_col, country_col in zip(city_columns, country_columns):
                    # Find rows with city but missing country
                    mask = (
                        chunk[city_col].notna() & 
                        chunk[city_col].astype(str).str.lower().ne('inconnu') & 
                        (chunk[country_col].isna() | chunk[country_col].astype(str).str.lower().eq('inconnu'))
                    )
                    
                    # Apply the mapping
                    for idx in chunk[mask].index:
                        city = chunk.loc[idx, city_col]
                        if isinstance(city, str):
                            city_lower = city.lower()
                            if city_lower in self.city_country_map:
                                chunk.loc[idx, country_col] = self.city_country_map[city_lower].title()
            
            # Replace 'inconnu' values with np.nan
            for col in chunk.columns:
                if chunk[col].dtype == object:  # Only process string columns
                    chunk[col] = chunk[col].replace({
                        '(?i)^inconnu$': np.nan,
                        '(?i)^inconnu,\\s*inconnu$': np.nan,
                        'inconnu, inconnu': np.nan
                    }, regex=True)
            
            # Standardize other text columns
            text_columns = [
                col for col in chunk.columns 
                if col not in ['user_id', 'first_name', 'last_name'] + city_columns
                and chunk[col].dtype == object
            ]
            
            for col in text_columns:
                chunk[col] = chunk[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
                
            # Generate new user_ids if requested (this could be made optional)
            if 'user_id' in chunk.columns:
                chunk['user_id'] = [abs(hash(uuid.uuid4())) for _ in range(len(chunk))]
                chunk['user_id'] = chunk['user_id'].astype('int64')
                
            return chunk
        except Exception as e:
            print(f"Error in process_chunk: {e}")
            # Return the original chunk if error occurs
            return chunk

    async def process_csv_in_chunks(self, 
                                  input_file: str, 
                                  output_file: str, 
                                  chunk_size: int = 100000,
                                  max_workers: int = None) -> None:
        """
        Process a large CSV file in chunks to handle memory constraints.
        Uses ProcessPoolExecutor for parallel processing of chunks.
        """
        print(f"🚀 Starting to process {input_file}")
        
        # First create the city-country map from the existing data
        self.city_country_map = await self.create_city_country_map(input_file)
        
        # Get the total number of rows for progress tracking
        try:
            total_rows = sum(1 for _ in open(input_file, 'r', encoding='utf-8')) - 1  # Subtract header
        except UnicodeDecodeError:
            # Try different encoding if UTF-8 fails
            try:
                total_rows = sum(1 for _ in open(input_file, 'r', encoding='latin-1')) - 1
            except Exception as e:
                print(f"Error counting rows: {e}")
                total_rows = 0  # Fallback
        
        total_chunks = max(1, total_rows // chunk_size + (1 if total_rows % chunk_size > 0 else 0))
        
        # Process the file in chunks
        try:
            reader = pd.read_csv(input_file, chunksize=chunk_size, low_memory=False, 
                                on_bad_lines='skip')
            
            # Write the header only once
            header = True
            
            # Track progress
            with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
                for i, chunk in enumerate(reader):
                    # Process chunk in parallel pool
                    try:
                        with ProcessPoolExecutor(max_workers=max_workers) as executor:
                            # This will execute process_chunk in a separate process
                            future = executor.submit(self.process_chunk, chunk)
                            cleaned_chunk = future.result()
                        
                        # Write the cleaned chunk to the output file
                        mode = 'w' if header else 'a'
                        cleaned_chunk.to_csv(output_file, index=False, mode=mode, header=header)
                        header = False
                    except Exception as e:
                        print(f"Error processing chunk {i}: {e}")
                    
                    pbar.update(1)
                    
            print(f"✅ CSV cleaning completed. Output saved to {output_file}")
            
            # Get final stats
            try:
                final_df = pd.read_csv(output_file, nrows=1)
                final_columns = len(final_df.columns)
                final_rows = sum(1 for _ in open(output_file, 'r', encoding='utf-8')) - 1  # Subtract header
                print(f"📊 Final data has {final_rows} rows and {final_columns} columns.")
            except Exception as e:
                print(f"❌ Error while getting final stats: {e}")
        
        except Exception as e:
            print(f"❌ Error processing CSV in chunks: {e}")

    async def enrich_with_geo_data(self, 
                                  input_file: str, 
                                  geo_json_file: str, 
                                  output_file: str,
                                  chunk_size: int = 100000) -> None:
        """
        Adds department and region columns to a CSV file based on a JSON lookup of French cities.
        Uses async IO for better performance with large files.
        """
        print("🌍 Starting geographical data enrichment...")
        
        # Load the city to department and region mapping from JSON file
        try:
            async with aiofiles.open(geo_json_file, 'r', encoding='utf-8') as json_file:
                content = await json_file.read()
                cities_data = json.loads(content)
            
            # Create a dictionary for fast lookup
            city_mapping = {
                city_info['city'].lower(): {
                    'department': city_info['department'],
                    'region': city_info['region']
                } for city_info in cities_data
            }
            
            print(f"🗺️ Loaded geo mapping for {len(city_mapping)} cities")
        except Exception as e:
            print(f"❌ Error loading geo data: {e}")
            return
            
        # Add fuzzy matching for cities not found directly
        city_keys = list(city_mapping.keys())
        
        # Process the file in chunks
        temp_dir = "temp_enriched_chunks"
        os.makedirs(temp_dir, exist_ok=True)
        temp_files = []
        
        # Get column information from the first chunk
        first_chunk = pd.read_csv(input_file, nrows=1)
        headers = list(first_chunk.columns)
        
        # Find city column index
        city_col_idx = None
        for i, header in enumerate(headers):
            if 'current_city' in header.lower():
                city_col_idx = i
                break
                
        if city_col_idx is None:
            print("❌ Error: Couldn't find city column in CSV file")
            return
            
        # Add new columns to headers
        headers.extend(['current_department', 'current_region'])
        
        # Calculate total chunks for progress bar
        try:
            total_rows = sum(1 for _ in open(input_file, 'r', encoding='utf-8')) - 1  # Subtract header
        except UnicodeDecodeError:
            # Try different encoding if UTF-8 fails
            try:
                total_rows = sum(1 for _ in open(input_file, 'r', encoding='latin-1')) - 1
            except Exception as e:
                print(f"Error counting rows: {e}")
                total_rows = 0  # Fallback
        
        total_chunks = total_rows // chunk_size + (1 if total_rows % chunk_size > 0 else 0)
        
        # Using standard tqdm for progress tracking (not async version)
        progress_bar = tqdm(total=total_chunks, desc="Enriching chunks")
        
        # Process each chunk
        for chunk_idx in range(total_chunks):
            skip_rows = 1 + chunk_idx * chunk_size  # Skip header + previous chunks
            if chunk_idx > 0:
                skip_rows = chunk_idx * chunk_size
                
            # Read chunk
            try:
                if chunk_idx == 0:
                    chunk_df = pd.read_csv(input_file, nrows=chunk_size)
                else:
                    chunk_df = pd.read_csv(input_file, skiprows=skip_rows, nrows=chunk_size, header=None)
                    chunk_df.columns = headers[:len(chunk_df.columns)]
                
                # Add new columns
                chunk_df['current_department'] = ""
                chunk_df['current_region'] = ""
                
                # Process cities in chunk
                for idx, row in chunk_df.iterrows():
                    if city_col_idx < len(row) and pd.notna(row[city_col_idx]):
                        city = str(row[city_col_idx]).strip().lower()
                        
                        # Direct lookup
                        if city in city_mapping:
                            chunk_df.at[idx, 'current_department'] = city_mapping[city]['department']
                            chunk_df.at[idx, 'current_region'] = city_mapping[city]['region']
                        else:
                            # Try fuzzy matching if not found
                            match_result = process.extractOne(
                                city, 
                                city_keys,
                                scorer=fuzz.token_sort_ratio,
                                score_cutoff=90
                            )
                            
                            if match_result:
                                matched_city = match_result[0]
                                chunk_df.at[idx, 'current_department'] = city_mapping[matched_city]['department']
                                chunk_df.at[idx, 'current_region'] = city_mapping[matched_city]['region']
                
                # Save enriched chunk to temp file
                temp_file = f"{temp_dir}/chunk_{chunk_idx}.csv"
                temp_files.append(temp_file)
                chunk_df.to_csv(temp_file, index=False)
                
                progress_bar.update(1)
            except Exception as e:
                print(f"Error processing chunk {chunk_idx}: {e}")
                
        progress_bar.close()
        
        # Combine all temporary files
        print("📋 Combining chunks into final output file...")
        
        with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
            for i, temp_file in enumerate(tqdm(temp_files, desc="Merging chunks")):
                with open(temp_file, 'r', encoding='utf-8') as infile:
                    # Only include header from first file
                    if i > 0:
                        next(infile)  # Skip header
                    outfile.write(infile.read())
                
                try:
                    # Clean up temp file
                    os.remove(temp_file)
                except Exception as e:
                    print(f"Error removing temp file {temp_file}: {e}")
                
        # Clean up temp directory
        try:
            os.rmdir(temp_dir)
        except Exception as e:
            print(f"Error removing temp directory: {e}")
        
        print(f"✅ Enriched data saved to {output_file}")
        
    async def run_complete_pipeline(self, 
                                   input_file: str,
                                   output_file: str = "phonora_cleaned_data.csv",
                                   enriched_output_file: str = "phonora_enriched_data.csv",
                                   geo_json_file: str = None,
                                   chunk_size: int = 100000,
                                   max_workers: int = None):
        """
        Run the complete cleaning and enrichment pipeline
        """
        print("🚀 Starting Phonora data cleaning and enrichment pipeline...")
        
        # Setup correction dictionaries
        await self.setup_correction_data()
        
        try:
            # Clean the data
            await self.process_csv_in_chunks(
                input_file=input_file,
                output_file=output_file,
                chunk_size=chunk_size,
                max_workers=max_workers
            )
            
            # Check if the output file was created successfully
            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                # Enrich with geo data if json file is provided
                if geo_json_file and os.path.exists(geo_json_file):
                    await self.enrich_with_geo_data(
                        input_file=output_file,
                        geo_json_file=geo_json_file,
                        output_file=enriched_output_file,
                        chunk_size=chunk_size
                    )
                else:
                    print(f"⚠️ Warning: Geo JSON file not found or not specified. Skipping enrichment step.")
            else:
                print(f"⚠️ Warning: Output file {output_file} was not created or is empty. Skipping enrichment step.")
            
            print("🏁 Pipeline execution completed successfully!")
        except Exception as e:
            print(f"❌ Error in pipeline execution: {e}")
        
# Main execution
if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Clean and enrich Phonora data')
    parser.add_argument('--input', '-i', default="phonora-personal-french-number-data.csv", 
                        help='Input CSV file path')
    parser.add_argument('--output', '-o', default="phonora_cleaned_data.csv", 
                        help='Output CSV file path')
    parser.add_argument('--enriched-output', '-e', default="phonora_enriched_data.csv", 
                        help='Enriched output CSV file path')
    parser.add_argument('--geo-json', '-g', default=None, 
                        help='Path to geo JSON file with city-department-region mappings')
    parser.add_argument('--chunk-size', '-c', type=int, default=100000, 
                        help='Chunk size for processing')
    parser.add_argument('--workers', '-w', type=int, default=None, 
                        help='Maximum number of worker processes')
    
    args = parser.parse_args()
    
    # Check if input file exists
    if not os.path.exists(args.input):
        print(f"⚠️ Warning: Input file '{args.input}' not found.")
        alternative_files = [f for f in os.listdir('.') if f.startswith('phonora') and f.endswith('.csv')]
        
        if alternative_files:
            print(f"Found alternative input files: {alternative_files}")
            args.input = alternative_files[0]
            print(f"Using '{args.input}' instead.")
        else:
            print("No alternative input files found. Please provide correct file path.")
            exit(1)
    
    # Run the pipeline
    cleaner = PhonoraDataCleaner()
    
    asyncio.run(cleaner.run_complete_pipeline(
        input_file=args.input,
        output_file=args.output,
        enriched_output_file=args.enriched_output,
        geo_json_file=args.geo_json,
        chunk_size=args.chunk_size,
        max_workers=args.workers
    ))