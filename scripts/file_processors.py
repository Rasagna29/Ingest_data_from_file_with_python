import os
import time
import duckdb
import pandas as pd
import json

def normalize_datetime(column):
    # Attempt parsing without format specification (pandas will infer the format)
    column = pd.to_datetime(column, errors='coerce')
    
    # If there are still NaT values, you can try a fallback logic
    # (for example, handling specific known date formats manually)
    
    # Optionally, handle missing or corrupted values if needed
    column = column.fillna(pd.NaT)  # Ensures that NaT values remain unchanged
    return column


def validate_columns(df, expected_columns):
    """
    Validate that the DataFrame has the expected columns in the correct order.
    """
    if list(df.columns) != expected_columns:
        raise ValueError(f"Invalid columns. Expected: {expected_columns}, Got: {list(df.columns)}")


def process_order_file(file_path, conn):
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Define the expected columns for the orders table
        expected_columns = [
            'orderId', 'sourceOrderId', 'deliveryDate', 'orderItems',
            'shippingFromAddress', 'shippingToAddress', 'pickingDeadline',
            'packingDeadline', 'shippingDeadline'
        ]
        validate_columns(df, expected_columns)


        # Convert orderItems, shippingFromAddress, and shippingToAddress to JSON strings
# JSON Serialisation in python uses pretty formatting with \n while serialisation. To avoid that use separators
        df['orderItems'] = df['orderItems'].apply(lambda x: json.dumps(eval(x), separators=(',', ':')))
        df['shippingFromAddress'] = df['shippingFromAddress'].apply(lambda x: json.dumps(eval(x), separators=(',', ':')))
        df['shippingToAddress'] = df['shippingToAddress'].apply(lambda x: json.dumps(eval(x), separators=(',', ':')))

        # Insert the data into the 'orders' table
        conn.executemany(
            """
            INSERT INTO orders (
                orderId, sourceOrderId, deliveryDate, orderItems, 
                shippingFromAddress, shippingToAddress, pickingDeadline, packingDeadline, shippingDeadline
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            df[['orderId', 'sourceOrderId', 'deliveryDate', 'orderItems', 'shippingFromAddress', 'shippingToAddress', 'pickingDeadline', 'packingDeadline', 'shippingDeadline']].values.tolist()
        )
        print(f"Processed and inserted data from: {file_path}")

      # Delete the file after successful processing
        os.remove(file_path)
        print(f"Deleted file: {file_path}")

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

def process_order_states_file(file_path, conn):
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Define the expected columns for the order_states table
        expected_columns = ['orderId', 'orderStatus', 'statusExecutedDttm']
        validate_columns(df, expected_columns)

        # Normalize datetime fields
        df['statusExecutedDttm'] = normalize_datetime(df['statusExecutedDttm'])


        # Insert the data into the 'order_states' table
        conn.executemany(
            """
            INSERT INTO order_states (
                orderId, orderStatus, statusExecutedDttm
            ) VALUES (?, ?, ?)
            """,
            df[['orderId', 'orderStatus', 'statusExecutedDttm']].values.tolist()
        )
        print(f"Processed and inserted data from: {file_path}")

        # Delete the file after successful processing
        os.remove(file_path)
        print(f"Deleted file: {file_path}")

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
