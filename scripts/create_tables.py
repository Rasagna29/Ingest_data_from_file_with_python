import duckdb

# Connect to DuckDB 
conn = duckdb.connect('/Users/rasagna/ingest_data_from_file_with_python/data/dynamic_state_ingestion_database.duckdb')

# Create a sequence for generating order IDs
conn.execute("""
    CREATE SEQUENCE IF NOT EXISTS order_id_seq START 1;
""")

# Create the 'orders' table with deadlines
conn.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        orderId VARCHAR DEFAULT 'O' || LPAD(nextval('order_id_seq')::VARCHAR, 9, '0'),
        sourceOrderId VARCHAR,
        deliveryDate DATE,
        orderItems JSON,
        shippingFromAddress JSON,
        shippingToAddress JSON,
        pickingDeadline TIMESTAMP,
        packingDeadline TIMESTAMP,
        shippingDeadline TIMESTAMP,
        PRIMARY KEY (orderId)
    );
""")

# Create the 'order_states' table without deadlines
conn.execute("""
    CREATE TABLE IF NOT EXISTS order_states (
        stateId INTEGER DEFAULT nextval('order_id_seq'),
        orderId VARCHAR REFERENCES orders(orderId),
        orderStatus VARCHAR,
        statusExecutedDttm TIMESTAMP,
        PRIMARY KEY (stateId)
    );
""")

print("Tables 'orders' and 'order_states' created successfully.")

# Close the connection
conn.close()
