import re
import json
import boto3
import string
import logging
import psycopg2
import pandas as pd
from io import StringIO
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO) # Priority of logs

# AWS S3 client
s3_client = boto3.client('s3')

# AWS S3 Configuration
TARGET_BUCKET = "e-commerce-processed"

# ! Helper functions
def clean_date(date):
    """Clean the date field by converting from mm/dd/yyyy to yyyy-mm-dd."""
    try:
        # Try parsing as mm/dd/yyyy
        parsed_date = datetime.strptime(date, "%m/%d/%Y")
        return parsed_date.strftime("%Y-%m-%d")
    except ValueError:
        return "1900-01-01"  # Default date

def clean_numeric(value):
    """ Clean the numeric field by removing all non-numeric characters. """
    return re.sub(r'\D', '', str(value))

# ! Data Cleaning Functions
def clean_customers(df):
    """ Clean the AdventureWorks Customers data. """
    logger.info("Cleaning customers data...")
    
    # Rename LastNa field to LastName
    df.rename(columns={'LastNa': 'LastName'}, inplace=True)

    # Group Education Field and replace its value with 'College Degree'
    df['EducationLevel'] = 'College Degree'

    # Change marital status codes: M - Married, S - Single
    df['MaritalStatus'] = df['MaritalStatus'].replace({'M': 'Married', 'S': 'Single'})

    # Clean up the prefix MrR to MR
    df['Prefix'] = df['Prefix'].replace({'MrR': 'MR'})

    # Remove all numbers from FirstName field
    df['FirstName'] = df['FirstName'].apply(lambda x: ''.join([i for i in x if not i.isdigit()]))

    # Remove all punctuations from the Occupation field
    df['Occupation'] = df['Occupation'].apply(lambda x: x.translate(str.maketrans('', '', string.punctuation)))

    # Split email address into parts and remove the characters before the @ symbol
    df['EmailAddress'] = df['EmailAddress'].apply(lambda x: x.split('@')[1])
    
    # Apply the clean_date function to the BirthDate field
    df['BirthDate'] = df['BirthDate'].apply(clean_date)
    
    # Apply the clean_numeric function to the numeric fields
    df['CustomerKey'] = df['CustomerKey'].apply(clean_numeric)
    
    # Change Home Owner field to boolean
    df['HomeOwner'] = df['HomeOwner'].replace('Y', True).replace('N', False).fillna(False)
    df['HomeOwner'] = df['HomeOwner'].apply(lambda x: x if x in [True, False] else False)
    df['HomeOwner'] = df['HomeOwner'].astype(bool)
    
    # Drop rows without a CustomerKey
    df = df.dropna(subset=['CustomerKey'])
    
    logger.info("Data cleaning complete for customers data.")
    return df

def cleanup_customers_new(df):
    """Clean the AdventureWorks Customers New data."""
    logger.info("Cleaning social media data...")
    
    # Remove rows where 'CustomerKey' is null
    df = df.dropna(subset=['CustomerKey'])
    
    # Fill null values in 'Social Media Accounts' with 'NoSocialMedia'
    df['Social Media Accounts'] = df['Social Media Accounts'].fillna('NoSocialMedia')
    
    # Split 'Social Media Accounts' into separate columns
    social_media_df = df['Social Media Accounts'].str.get_dummies(sep=', ')    
    # Concatenate with the original dataframe
    df = pd.concat([df[['CustomerKey']], social_media_df], axis=1)
    
    logger.info("Data cleaning complete for customers social media data.")
    return df

def cleanup_sales(df):
    """ Clean any of the the AdventureWorks Sales datasets. """
    logger.info("Cleaning customer sales data...")
    
    # Rename the last column to 'OrderQuantity' if necessary
    if df.columns[-1] != "OrderQuantity":
        df.rename(columns={df.columns[-1]: "OrderQuantity"}, inplace=True)
        
    # Remove rows with missing 'OrderQuantity' values
    df = df.dropna(subset=['OrderQuantity'])
    
    # Remove all non-numeric characters from numeric fields
    df['OrderQuantity'] = df['OrderQuantity'].apply(clean_numeric)
    df['ProductKey'] = df['ProductKey'].apply(clean_numeric)
    df['CustomerKey'] = df['CustomerKey'].apply(clean_numeric)
    df['TerritoryKey'] = df['TerritoryKey'].apply(clean_numeric)
    df['OrderLineItem'] = df['OrderLineItem'].apply(clean_numeric)
    
    # Clean date fields
    df["OrderDate"] = df["OrderDate"].apply(clean_date)
    df["StockDate"] = df["StockDate"].apply(clean_date)
    
    # Extract year from OrderDate and store in a new column
    df["OrderYear"] = pd.to_datetime(df["OrderDate"]).dt.year
    
    logger.info("Data cleaning complete for customer sales data.")
    return df

def cleanup_returns(df):
    """ Clean the AdventureWorks Returns data. """
    logger.info("Cleaning returns data...")

    # Clean date field
    df["ReturnDate"] = df["ReturnDate"].apply(clean_date)
    
    # Clean numeric fields
    df["TerritoryKey"] = df["TerritoryKey"].apply(clean_numeric)
    df["ProductKey"] = df["ProductKey"].apply(clean_numeric)
    df["ReturnQuantity"] = df["ReturnQuantity"].apply(clean_numeric).astype(int)
    
    # Drop rows where ReturnQuantity < 1
    df = df[df["ReturnQuantity"] >= 1]
    
    logger.info("Data cleaning complete for returns data.")
    return df

def cleanup_products(df):
    """ Clean the AdventureWorks Products data. """
    logger.info("Cleaning products data...")

    # Drop rows with missing ProductKey
    df = df.dropna(subset=["ProductKey"])

    # Clean numeric fields
    df["ProductKey"] = df["ProductKey"].apply(clean_numeric)
    df["ProductSubcategoryKey"] = df["ProductSubcategoryKey"].apply(clean_numeric)
    df["ProductCost"] = df["ProductCost"].apply(clean_numeric)
    df["ProductPrice"] = df["ProductPrice"].apply(clean_numeric)
    
    # Fill missing values with appropriate defaults
    df["ProductSKU"] = df["ProductSKU"].fillna("Unknown")
    df["ProductName"] = df["ProductName"].fillna("Unknown")
    df["ModelName"] = df["ModelName"].fillna("Unknown")
    df["ProductDescription"] = df["ProductDescription"].fillna("No Description")
    df["ProductColor"] = df["ProductColor"].fillna("NA")
    df["ProductSize"] = df["ProductSize"].fillna("NA")
    df["ProductStyle"] = df["ProductStyle"].fillna("NA")
    
    # Fill '0' in 'ProductSize' and 'ProductStyle' columns with 'NA'
    df["ProductSize"] = df["ProductSize"].replace('0', 'NA')
    df["ProductStyle"] = df["ProductStyle"].replace('0', 'NA')

    logger.info("Data cleaning complete for products data.")    
    return df

def clean_data(df, filename):
    """ Clean the DataFrame before processing. """
    logger.info("Initializing data cleanup...")
    
    # Cleanup functions for each file
    cleanup_funcs = {
        "customers.csv": clean_customers,
        "customers_new.csv": cleanup_customers_new,
        "sales_2015.csv": cleanup_sales,
        "sales_2016.csv": cleanup_sales,
        "sales_2017.csv": cleanup_sales,
        "returns.csv": cleanup_returns,
        "products.csv": cleanup_products,
    }

    # Choose the type of processing required based on the filename
    if filename in cleanup_funcs:
        logger.info("Cleaning data for file: %s", filename)
        df = cleanup_funcs[filename](df)
    else:
        logger.info("No cleaning required for file: %s", filename)

    logger.info("Data cleaning complete. New shape of the dataframe: %s", df.shape)
    return df

# ! Lambda Handler Function to process the S3 event
def lambda_handler(event, context):
    """ Extract data from S3, clean it, and load it into Redshift. """
    logger.info("Lambda function triggered with event: %s", event)

    try:
        # Extract bucket name and file path from event object
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        file_path = record['s3']['object']['key']
        filename = file_path.split('/')[-1]
        
        logger.info("Bucket name: %s, File path: %s", bucket_name, file_path)

        obj = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        logger.info("Object: %s", obj)

        logger.info("Successfully read file from S3: %s", file_path)

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=file_path)
        if "Contents" not in response:
            raise ValueError(f"File {file_path} not found in bucket {bucket_name}")

        data = obj["Body"].read().decode("ISO-8859-1")

        logger.info("Successfully read file from S3: %s", file_path)
        
        # Convert CSV data to Pandas DataFrame
        logger.info("Converting CSV data to DataFrame")
        df = pd.read_csv(StringIO(data))
        logger.info("CSV file loaded into DataFrame. Columns: %s", df.columns.tolist())
        
        # Clean the data
        df = clean_data(df, filename)

        # Redshift Credentials
        host = "your-redshift-cluster-endpoint"
        dbname = "your-database-name"
        user = "your-username"
        password = "your-password"
        
        # Get last word in file path for the table name
        tablename = filename.split('.')[0]
        
        # Save cleaned data to a temporary CSV file
        logger.info("Saving cleaned data to a temporary CSV file")
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)
        logger.info("Cleaned data saved to temporary CSV file.")

        # Upload cleaned data to another S3
        logger.info("Uploading cleaned data to another S3 bucket")
        target_filename = f"{tablename}_processed.csv"
        s3_client.put_object(Bucket=TARGET_BUCKET, Key=target_filename, Body=csv_buffer.getvalue())
        logger.info("Cleaned data uploaded to S3 bucket: %s", TARGET_BUCKET)
        
        # Establish connection to Redshift
        logger.info("Establishing connection to Redshift database: %s", dbname)
        connection = psycopg2.connect(dbname=dbname, host=host, port='5439', user=user, password=password)
        curs = connection.cursor()
        logger.info("Successfully connected to Redshift database: %s", dbname)
        
        # Upload cleaned data to Redshift
        copy_query = """
            COPY {}
            FROM 's3://{}/{}'
            IAM_ROLE 'your-iam-role-arn'
            FORMAT AS CSV
            DELIMITER ',';
        """.format(tablename, TARGET_BUCKET, target_filename)
        
        logger.info("Executing Redshift COPY command...")
        curs.execute(copy_query)
        connection.commit()
        
        # Close connection
        curs.close()
        connection.close()
        
        logger.info("Data successfully loaded into Redshift table: %s", tablename)
        
        return {
            "statusCode": 200,
            "body": "Data cleaned and uploaded to Redshift successfully."
        }
    
    except Exception as e:
        logger.error("Error occurred: %s", str(e), exc_info=True)
        return {
            "statusCode": 500,
            "body": f"Error processing data: {str(e)}"
        }
