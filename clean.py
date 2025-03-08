import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from datetime import datetime

def load_unclean_data(server_name, database_name, table_name):
    """
    Load unclean data from SQL Server database

    Args:
    server_name (str): Name of the SQL Server instance
    database_name (str): Name of the database
    table_name (str): Name of the table to load data from

    Returns:
    pandas.DataFrame: Loaded data or None if loading fails
    """
    print("Loading unclean data from SQL Server...")

    try:
        # Create a connection string for SQLAlchemy using pyodbc driver
        # Trusted_Connection=yes means using Windows Authentication
        conn_str = f"DRIVER={{SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;"

        # URL encode the connection string to handle special characters
        quoted_conn_str = quote_plus(conn_str)

        # Create SQLAlchemy engine connection string
        engine_str = f"mssql+pyodbc:///?odbc_connect={quoted_conn_str}"

        # Create database engine
        engine = create_engine(engine_str)

        # Execute SQL query to select all records from the specified table
        query = f"SELECT * FROM {table_name}"
        data = pd.read_sql(query, engine)

        print(f"Successfully loaded {len(data)} records from {database_name}.{table_name}")
        return data

    except Exception as e:
        # Handle and log any errors during data loading
        print(f"Error loading data from SQL Server: {str(e)}")
        return None

def clean_data(data):
    """
    Clean and transform the input data

    Args:
    data (pandas.DataFrame): Raw input data to be cleaned

    Returns:
    pandas.DataFrame: Cleaned and transformed data
    """
    print("Cleaning and transforming data...")

    # Convert CustomerID to numeric, coercing errors to NaN
    data['CustomerID'] = pd.to_numeric(data['CustomerID'], errors='coerce')

    # Clean Name: strip whitespace and convert to title case
    data['Name'] = data['Name'].str.strip().str.title()

    # Age cleaning function: handles various age input formats
    def clean_age(age):
        if pd.isna(age) or age == '':
            return None

        # Log invalid age values that don't contain digits
        if not any(c.isdigit() for c in str(age)):
            print(f"Invalid Age: {age}")
            return None

        # Extract only digits from age input
        digit_part = ''.join(c for c in str(age) if c.isdigit())
        if digit_part:
            return int(digit_part)  # Convert to integer
        return None

    # Apply age cleaning
    data['Age'] = data['Age'].apply(clean_age)

    # Gender standardization function
    def standardize_gender(gender):
        if pd.isna(gender) or gender == '':
            return None
        gender = str(gender).strip().lower()
        if gender in ['m', 'male']:
            return 'Male'
        elif gender in ['f', 'female']:
            return 'Female'
        return None

    # Apply gender standardization
    data['Gender'] = data['Gender'].apply(standardize_gender)

    # Location and State extraction function
    def extract_state(location):
        if pd.isna(location) or location == '':
            return None, None

        location = str(location).strip()

        # Handle city, state format (comma-separated)
        if ',' in location:
            parts = location.split(',')
            city_part = parts[0].strip()
            state_part = parts[1].strip() if len(parts) > 1 else None

            # Standardize state part
            if state_part and len(state_part) > 2:
                state_part = state_part[:2].upper()
            elif state_part:
                state_part = state_part.upper()

            return location, state_part

        # Handle city/state format (slash-separated)
        elif '/' in location:
            parts = location.split('/')
            city_part = parts[0].strip()
            state_part = parts[1].strip() if len(parts) > 1 else None

            # Standardize state part
            if state_part and len(state_part) > 2:
                state_part = state_part[:2].upper()
            elif state_part:
                state_part = state_part.upper()

            return f"{city_part}, {state_part}", state_part

        # If no state information found
        return location, None

    # Apply location and state extraction
    data['State'] = None
    data[['Location', 'State']] = data.apply(lambda row: pd.Series(extract_state(row['Location'])), axis=1)

    # Date cleaning function with multiple format support
    def clean_date(date_str):
        if pd.isna(date_str) or date_str == '':
            return None

        date_str = str(date_str).strip()

        # List of possible date formats to try
        date_formats = [
            '%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%Y.%m.%d',
            '%B %d, %Y', '%m-%d-%y', '%d/%m/%Y', '%m/%d/%y'
        ]

        # Try parsing date with different formats
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        return None

    # Apply date cleaning
    data['PurchaseDate'] = data['PurchaseDate'].apply(clean_date)

    # Extract purchase month abbreviation
    data['PurchaseMonth'] = data['PurchaseDate'].apply(lambda x: x.strftime('%b') if x is not None else None)

    # Clean ProductCategory: strip and title case
    data['ProductCategory'] = data['ProductCategory'].str.strip().str.title()

    # Amount spent cleaning function
    def clean_amount(amount):
        if pd.isna(amount) or amount == '':
            return None

        amount_str = str(amount).strip()

        # Log invalid amount values
        if not any(c.isdigit() for c in amount_str):
            print(f"Invalid Amount: {amount}")
            return None

        # Remove currency symbols
        amount_str = amount_str.replace('$', '').replace('€', '').replace('£', '')

        # Replace comma with dot for decimal
        amount_str = amount_str.replace(',', '.')

        # Extract valid numeric characters
        cleaned = ''
        decimal_count = 0

        for char in amount_str:
            if char.isdigit():
                cleaned += char
            elif char == '.' and decimal_count == 0:
                cleaned += '.'
                decimal_count += 1

        try:
            return float(cleaned) if cleaned else None
        except ValueError:
            return None

    # Apply amount cleaning
    data['AmountSpent'] = data['AmountSpent'].apply(clean_amount)

    # Age group categorization function
    def get_age_group(age):
        if pd.isna(age):
            return None
        elif age < 18:
            return 'Youth'
        elif age < 35:
            return 'Young'
        elif age < 50:
            return 'Middle'
        else:
            return 'Senior'

    # Add age group column
    data['AgeGroup'] = data['Age'].apply(get_age_group)

    return data

def write_clean_data_to_sql(data, server_name, database_name, table_name):
    """
    Write cleaned data to SQL Server

    Args:
    data (pandas.DataFrame): Cleaned data to write
    server_name (str): SQL Server instance name
    database_name (str): Target database name
    table_name (str): Target table name

    Returns:
    bool: Success status of data writing
    """
    print(f"Writing cleaned data to SQL Server: {server_name}, Database: {database_name}, Table: {table_name}")

    try:
        # Create connection string similar to load_unclean_data
        conn_str = f"DRIVER={{SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;"
        quoted_conn_str = quote_plus(conn_str)
        engine_str = f"mssql+pyodbc:///?odbc_connect={quoted_conn_str}"

        # Create database engine
        engine = create_engine(engine_str)

        # Write data to SQL Server
        # if_exists='replace' means drop and recreate the table
        data.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Successfully wrote {len(data)} records to {database_name}.{table_name}")
        return True

    except Exception as e:
        # Handle and log any errors during data writing
        print(f"Error writing to SQL Server: {str(e)}")
        return False

def main():
    """
    Main function to orchestrate data cleaning process
    """
    # Configuration parameters
    server = r"LAPTOP-D00LK2I0\SQLEXPRESS01"  # SQL Server instance
    database = "Customer Analysis"  # Database name
    unclean_table = "UncleanCustomers"  # Source table with raw data
    clean_table = "CleanedCustomers"  # Destination table for cleaned data

    # Load unclean data from SQL Server
    unclean_data = load_unclean_data(server, database, unclean_table)

    # Exit if data loading fails
    if unclean_data is None:
        print("Error loading unclean data. Exiting.")
        return

    # Clean the loaded data
    cleaned_data = clean_data(unclean_data)

    # Write cleaned data back to SQL Server
    success = write_clean_data_to_sql(cleaned_data, server, database, clean_table)

    # Print final status
    if success:
        print("\nProcess completed successfully!")
        print(f"Cleaned data has been imported into {database} on {server} in the {clean_table} table.")
    else:
        print("\nProcess completed with errors.")
        print("Check the error messages above and verify your SQL Server connection details.")

if __name__ == "__main__":
    main()
