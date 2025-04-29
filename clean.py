#!/usr/bin/env python3
"""
Database cleaning script for standardizing and cleaning database content.
"""
from sqlalchemy.orm import Session
import argparse
import datetime
import logging
import pandas as pd
from sqlalchemy import create_engine, inspect, text
import sys
from datetime import datetime

class CleaningOperations:
    """Operations for cleaning database content."""
    
    def __init__(self, engine, inspector, tables, logger):
        """
        Initialize cleaning operations.
        
        Args:
            engine: SQLAlchemy engine
            inspector: SQLAlchemy inspector
            tables (list): List of table names
            logger: Logger instance
        """
        self.engine = engine
        self.inspector = inspector
        self.tables = tables
        self.logger = logger
    
    def clean_email_formats(self, email_columns):
        """
        Clean and standardize email formats.
        
        Args:
            email_columns (list): List of tuples (table_name, column_name)
        """
        for table, column in email_columns:
            try:
                # Trim whitespace
                query = f"""
                UPDATE {table}
                SET {column} = TRIM({column})
                WHERE {column} IS NOT NULL
                """
                with Session(self.engine) as session:
                    session.execute(text(query))
                    session.commit()
                # Convert to lowercase
                query = f"""
                UPDATE {table}
                SET {column} = LOWER({column})
                WHERE {column} IS NOT NULL
                """
                with Session(self.engine) as session:
                    session.execute(text(query))
                    session.commit()
                
                self.logger.info(f"Cleaned email formats in {table}.{column}")
            except Exception as e:
                self.logger.error(f"Error cleaning emails in {table}.{column}: {str(e)}")


    
    def clean_phone_formats(self, phone_columns):
        """
        Clean and standardize phone number formats.
        
        Args:
            phone_columns (list): List of tuples (table_name, column_name)
        """
        for table, column in phone_columns:
            try:
                # Remove non-digit characters except + at the beginning
                query = f"""
                UPDATE {table}
                SET {column} = REGEXP_REPLACE(REGEXP_REPLACE({column}, '^\\+', 'PLUS_PLACEHOLDER'), '[^0-9]', '', 'g')
                WHERE {column} IS NOT NULL
                """
                with Session(self.engine) as session:
                    session.execute(text(query))
                    session.commit()
                
                # Restore + sign
                query = f"""
                UPDATE {table}
                SET {column} = REGEXP_REPLACE({column}, '^PLUS_PLACEHOLDER', '+')
                WHERE {column} LIKE 'PLUS_PLACEHOLDER%'
                """
                with Session(self.engine) as session:
                    session.execute(text(query))
                    session.commit()
                
                self.logger.info(f"Cleaned phone formats in {table}.{column}")
            except Exception as e:
                self.logger.error(f"Error cleaning phones in {table}.{column}: {str(e)}")
    
    def clean_text_fields(self):
        """Clean and standardize text fields (trim whitespace, fix capitalization)."""
        text_fields = []
        
        for table in self.tables:
            columns = self.inspector.get_columns(table, schema='public')
            for col in columns:
                if str(col['type']).startswith('VARCHAR') and 'name' in col:
                    text_fields.append((table, col['name']))
        
        for table, column in text_fields:
            try:
                # Trim whitespace
                query = f"""
                UPDATE {table}
                SET {column} = TRIM({column})
                WHERE {column} IS NOT NULL
                """
                with Session(self.engine) as session:
                    session.execute(text(query))
                    session.commit()
                
                # Proper capitalization for name fields
                if 'nom' in column.lower() or 'prenom' in column.lower() or 'name' in column.lower():
                    query = f"""
                    UPDATE {table}
                    SET {column} = INITCAP({column})
                    WHERE {column} IS NOT NULL
                    """
                    with Session(self.engine) as session:
                        session.execute(text(query))
                        session.commit()
                
                self.logger.info(f"Cleaned text field {table}.{column}")
            except Exception as e:
                self.logger.error(f"Error cleaning text field {table}.{column}: {str(e)}")
    
    def standardize_dates(self, date_columns):
        """
        Standardize date formats and fix obvious date errors.
        
        Args:
            date_columns (list): List of tuples (table_name, column_name)
        """
        from datetime import datetime
        
        for table, column in date_columns:
            try:
                # Fix future dates for historical fields
                if 'naissance' in column or 'creation' in column:
                    query = f"""
                    UPDATE {table}
                    SET {column} = NULL
                    WHERE {column} > CURRENT_DATE
                    """
                    with Session(self.engine) as session:
                        result = session.execute(text(query))
                        session.commit()
                    if result.rowcount > 0:
                        self.logger.info(f"Nullified {result.rowcount} future dates in {table}.{column}")
                
                # Fix logical date sequence errors
                if table == 'employe' and column == 'date_depart':
                    query = """
                    UPDATE employe
                    SET date_depart = NULL
                    WHERE date_depart < date_embauche
                    """
                    with Session(self.engine) as session:
                          result = session.execute(text(query))
            
                    if result.rowcount > 0:
                        self.logger.info(f"Nullified {result.rowcount} illogical departure dates that came before hire dates")
                
                self.logger.info(f"Standardized dates in {table}.{column}")
            except Exception as e:
                self.logger.error(f"Error standardizing dates in {table}.{column}: {str(e)}")
    
    def fill_missing_values(self):
        """Fill missing values with sensible defaults where appropriate."""
        default_values = [
            # Table, column, default value, condition
            ('entreprise', 'nbr_etablissements', 1, 'nbr_etablissements IS NULL'),
            ('entreprise', 'nbr_etablissements_overt', 1, 'nbr_etablissements_overt IS NULL AND nbr_etablissements = 1'),
            ('entreprise', 'nbr_etablissements_fermet', 0, 'nbr_etablissements_fermet IS NULL'),
            ('entreprise', 'capital_sociale', 0, 'capital_sociale IS NULL'),
            ('entreprise', 'chifre_affaire', 0, 'chifre_affaire IS NULL'),
            ('entreprise', 'effective', 0, 'effective IS NULL'),
            ('entreprise', 'devise', 'EUR', "devise IS NULL "),
            ('users', 'actif', True, 'actif IS NULL'),
        ]
        
        for table, column, default_value, condition in default_values:
            # Check if table exists
            if table not in self.tables:
                self.logger.info(f"Skipping filling missing values for {table}.{column}: table doesn't exist")
                continue
                
            # Check if column exists
            columns = self.inspector.get_columns(table)
            column_names = [col['name'] for col in columns]
            if column not in column_names:
                self.logger.info(f"Skipping filling missing values for {table}.{column}: column doesn't exist")
                continue
                
            try:
                if isinstance(default_value, str):
                    default_str = f"'{default_value}'"
                elif isinstance(default_value, bool):
                    default_str = 'TRUE' if default_value else 'FALSE'
                else:
                    default_str = str(default_value)
                
                query = f"""
                UPDATE {table}
                SET {column} = {default_str}
                WHERE {condition}
                """
                with Session(self.engine) as session:
                          result = session.execute(text(query))
                if result.rowcount > 0:
                    self.logger.info(f"Filled {result.rowcount} missing values in {table}.{column} with default: {default_value}")
            except Exception as e:
                self.logger.error(f"Error filling missing values in {table}.{column}: {str(e)}")


def setup_logger():
    """Set up and return a logger instance."""
    logger = logging.getLogger('db_cleaner')
    logger.setLevel(logging.INFO)
    
    # Create console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # Create file handler
    fh = logging.FileHandler('db_cleaner.log')
    fh.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(ch)
    logger.addHandler(fh)
    
    return logger
def main():
    sys.stdout.flush()  # Force output in PowerShell
    
    # Rest of your main() function...    
    """Main function to execute database cleaning operations."""
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Database cleaning script')
    parser.add_argument('--host', required=True, help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--user', required=True, help='Database user')
    parser.add_argument('--password', required=True, help='Database password')
    parser.add_argument('--schema', default='public', help='Database schema')
    
    args = parser.parse_args()
    
    # Set up logger
    logger = setup_logger()
    logger.info("Starting database cleaning operations")
    
    try:
        # Create SQLAlchemy engine
        conn_str = f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}"
        engine = create_engine(conn_str)
        logger.info(f"Connected to database {args.database} on {args.host}")
        
        # Create inspector
        inspector = inspect(engine)
        
        # Get tables
        tables = inspector.get_table_names(schema=args.schema)
        logger.info(f"Found {len(tables)} tables in schema {args.schema}")
        
        # Initialize cleaning operations
        cleaner = CleaningOperations(engine, inspector, tables, logger)
        
        # Define columns to clean
        email_columns = []
        phone_columns = []
        date_columns = []
        
        # Find columns based on names
        for table in tables:
            columns = inspector.get_columns(table, schema=args.schema)
            for col in columns:
                col_name = col['name'].lower()
                
                # Identify email columns
                if 'email' in col_name or 'mail' in col_name:
                    email_columns.append((table, col['name']))
                
                # Identify phone columns
                if 'phone' in col_name or 'tel' in col_name or 'mobile' in col_name:
                    phone_columns.append((table, col['name']))
                
                # Identify date columns
                if col['type'].python_type == datetime.date or 'date' in col_name:
                    date_columns.append((table, col['name']))
        
        # Execute cleaning operations
        logger.info("Starting email format cleaning")
        cleaner.clean_email_formats(email_columns)
        
        logger.info("Starting phone format cleaning")
        cleaner.clean_phone_formats(phone_columns)
        
        logger.info("Starting text field cleaning")
        cleaner.clean_text_fields()
        
        logger.info("Starting date standardization")
        cleaner.standardize_dates(date_columns)
        
        logger.info("Starting missing value filling")
        cleaner.fill_missing_values()
        
        logger.info("Database cleaning completed successfully")
    
    except Exception as e:
        logger.error(f"Error during database cleaning: {str(e)}")
        sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    exit(main())