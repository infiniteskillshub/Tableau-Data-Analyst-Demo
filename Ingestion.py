import random
import csv
import string
import os
import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_unclean_data(num_records=100):
    """
    Generate a dataset with intentional data quality issues

    Args:
        num_records (int): Number of records to generate

    Returns:
        list: Generated data with header and records, each with intentional imperfections
    """
    print("Generating unclean data...")

    # Predefined lists for random data generation
    # These lists provide a base for creating realistic but varied data
    first_names = ["John", "Jane", "Michael", "Emily", "David", "Sarah", "Robert", "Maria", "James", "Lisa",
                  "Thomas", "Jessica", "Daniel", "Jennifer", "Christopher", "Linda", "Matthew", "Patricia",
                  "Andrew", "Elizabeth"]

    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Garcia", "Rodriguez",
                 "Wilson", "Martinez", "Anderson", "Taylor", "Thomas", "Hernandez", "Moore", "Martin",
                 "Jackson", "Thompson", "White"]

    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio",
             "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "San Francisco", "Columbus",
             "Indianapolis", "Seattle", "Denver", "Washington DC", "Boston", "Nashville"]

    states = ["NY", "CA", "IL", "TX", "AZ", "PA", "FL", "OH", "GA", "NC", "WA", "CO", "DC", "MA", "TN", "VA"]

    product_categories = ["Electronics", "Fashion", "Home & Kitchen", "Sports", "Beauty", "Books", "Toys",
                         "Grocery", "Automotive", "Health", "Office Supplies", "Garden", "Pet Supplies"]

    # Initialize data list with header
    data = []
    header = ["CustomerID", "Name", "Age", "Gender", "Location", "PurchaseDate", "ProductCategory", "AmountSpent"]
    data.append(header)

    # Generate records with various data quality issues
    for i in range(1, num_records + 1):
        # CustomerID: Straightforward, sequential
        customer_id = i

        # Name: Introduces various potential data issues
        if random.random() < 0.15:  # Sometimes missing last name
            name = random.choice(first_names)
        elif random.random() < 0.1:  # Sometimes with middle initial
            name = f"{random.choice(first_names)} {random.choice(string.ascii_uppercase)}. {random.choice(last_names)}"
        else:
            name = f"{random.choice(first_names)} {random.choice(last_names)}"

        # Introduce name errors occasionally
        if random.random() < 0.3:
            if random.random() < 0.5 and len(name) > 1:
                # Random typo
                pos = random.randint(0, len(name) - 1)
                char = random.choice(string.ascii_letters)
                name = name[:pos] + char + name[pos+1:]
            else:
                # Case inconsistency
                if random.random() < 0.5:
                    name = name.lower()
                else:
                    name = name.upper()

        # Age: Introduces multiple data quality issues
        if random.random() < 0.1:  # Sometimes missing
            age = ''
        elif random.random() < 0.05:  # Sometimes invalid
            age = random.choice(['NA', 'N/A', '?', 'Unknown'])
        elif random.random() < 0.05:  # Sometimes with text
            age = f"{random.randint(18, 75)} years"
        else:
            age = str(random.randint(18, 75))

        # Gender: Introduces inconsistent formats and missing values
        gender_options = ['Male', 'Female', 'M', 'F', 'm', 'f', 'MALE', 'FEMALE', '']
        gender_weights = [0.3, 0.3, 0.1, 0.1, 0.05, 0.05, 0.05, 0.05, 0.1]  # 10% missing
        gender = random.choices(gender_options, weights=gender_weights)[0]

        # Location: Creates various location format issues
        if random.random() < 0.7:  # 70% city, state format
            city = random.choice(cities)
            state = random.choice(states)
            location = f"{city}, {state}"
        elif random.random() < 0.15:  # 15% city only
            location = random.choice(cities)
        elif random.random() < 0.1:  # 10% missing
            location = ''
        else:  # 5% unusual format
            location = f"{random.choice(cities)}/{random.choice(states)}"

        # Purchase Date: Generates dates with multiple formatting issues
        date_format_choice = random.random()
        purchase_date = datetime(2023, random.randint(1, 12), random.randint(1, 28))

        # Multiple date format variations
        if date_format_choice < 0.15:  # Missing
            purchase_date_str = ''
        elif date_format_choice < 0.3:  # MM/DD/YYYY
            purchase_date_str = purchase_date.strftime('%m/%d/%Y')
        elif date_format_choice < 0.45:  # DD-MM-YYYY
            purchase_date_str = purchase_date.strftime('%d-%m-%Y')
        elif date_format_choice < 0.6:  # YYYY.MM.DD
            purchase_date_str = purchase_date.strftime('%Y.%m.%d')
        elif date_format_choice < 0.75:  # Month DD, YYYY
            purchase_date_str = purchase_date.strftime('%B %d, %Y')
        elif date_format_choice < 0.9:  # MM-DD-YY
            purchase_date_str = purchase_date.strftime('%m-%d-%y')
        else:  # ISO format
            purchase_date_str = purchase_date.strftime('%Y-%m-%d')

        # Product Category: Introduces formatting and missing value issues
        product_category_choice = random.random()
        if product_category_choice < 0.1:  # Missing
            product_category = ''
        else:
            product_category = random.choice(product_categories)
            # Introduce case inconsistencies
            if random.random() < 0.3:
                if random.random() < 0.5:
                    product_category = product_category.lower()
                else:
                    product_category = product_category.upper()

        # Amount Spent: Creates various numeric formatting issues
        amount_choice = random.random()
        if amount_choice < 0.1:  # Missing
            amount_spent = ''
        elif amount_choice < 0.2:  # Invalid
            amount_spent = random.choice(['NA', 'N/A', '?', 'Unknown', 'TBD'])
        elif amount_choice < 0.4:  # Currency symbol
            amount_spent = f"${random.uniform(10, 1000):.2f}"
        elif amount_choice < 0.6:  # Integer
            amount_spent = str(int(random.uniform(10, 1000)))
        elif amount_choice < 0.8:  # Decimal with comma as decimal separator
            amount_spent = f"{int(random.uniform(10, 1000))},{int(random.uniform(0, 100)):02d}"
        else:  # Normal decimal
            amount_spent = f"{random.uniform(10, 1000):.2f}"

        # Compile the record with all its intentional imperfections
        record = [str(customer_id), name, age, gender, location, purchase_date_str, product_category, amount_spent]
        data.append(record)

    return data

def insert_into_sqlserver(data, server, database):
    """
    Insert generated unclean data into SQL Server database

    Args:
        data (list): Generated data to insert
        server (str): SQL Server instance name
        database (str): Target database name

    Returns:
        bool: Success status of data insertion
    """
    print(f"Connecting to SQL Server: {server}, Database: {database}...")

    # Create connection string for Windows Authentication
    conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

    try:
        # Establish database connection
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Drop existing table if it exists to start fresh
        cursor.execute("""
        IF OBJECT_ID('UncleanCustomers', 'U') IS NOT NULL
            DROP TABLE UncleanCustomers
        """)
        conn.commit()

        # Create table with flexible NVARCHAR columns to handle various data formats
        cursor.execute("""
        CREATE TABLE UncleanCustomers (
            CustomerID INT PRIMARY KEY,
            Name NVARCHAR(100),
            Age NVARCHAR(50),
            Gender NVARCHAR(50),
            Location NVARCHAR(100),
            PurchaseDate NVARCHAR(50),
            ProductCategory NVARCHAR(50),
            AmountSpent NVARCHAR(50)
        )
        """)
        conn.commit()

        print("Inserting data into SQL Server...")

        # Insert data row by row with error handling
        inserted_count = 0
        for record in data[1:]:  # Skip header row
            try:
                cursor.execute("""
                INSERT INTO UncleanCustomers (CustomerID, Name, Age, Gender, Location, PurchaseDate, ProductCategory, AmountSpent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, record)
                inserted_count += 1

                # Commit in batches for performance
                if inserted_count % 50 == 0:
                    conn.commit()
                    print(f"Inserted {inserted_count} records...")

            except Exception as e:
                print(f"Error inserting record {record[0]}: {e}")
                continue

        # Final commit to ensure all data is saved
        conn.commit()
        print(f"Successfully inserted {inserted_count} records into the UncleanCustomers table.")

        # Sample and display a few records to verify insertion
        cursor.execute("SELECT TOP 5 * FROM UncleanCustomers")
        rows = cursor.fetchall()
        print("\nSample data from the UncleanCustomers table:")
        for row in rows:
            print(row)

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error connecting to or inserting into SQL Server: {e}")
        return False

    return True

def main():
    """
    Main function to orchestrate the unclean data generation and insertion process
    """
    # Configuration parameters
    server = r"LAPTOP-D00LK2I0\SQLEXPRESS01"  # SQL Server instance
    database = "Customer Analysis"  # Target database
    num_records = 200  # Number of records to generate

    # Print process details
    print(f"Starting unclean data generation and import process...")
    print(f"Target: SQL Server {server}, Database: {database}")
    print(f"Number of records to generate: {num_records}")

    # Generate unclean data
    unclean_data = generate_unclean_data(num_records)
    print(f"Generated {len(unclean_data)-1} records of unclean data")

    # Insert data into SQL Server
    success = insert_into_sqlserver(unclean_data, server, database)

    # Print final status
    if success:
        print("\nProcess completed successfully!")
        print(f"Data has been imported into {database} on {server} in the UncleanCustomers table.")
    else:
        print("\nProcess completed with errors.")
        print("Check the error messages above and verify your SQL Server connection details.")

if __name__ == "__main__":
    main()
