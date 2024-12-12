import os
import time
import duckdb
from file_processors import process_order_file, process_order_states_file

def watch_directory(directory, conn):
    print(f"Watching directory: {directory}")

    while True:
        # Gather all files in the directory
        csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]

        # Prioritize order files over order_states files
        order_files = [f for f in csv_files if f.startswith('order_data')]
        order_states_files = [f for f in csv_files if f.startswith('order_states')]

        # Process order files first
        for file_name in order_files + order_states_files:
            file_path = os.path.join(directory, file_name)

            try:
                if file_name.startswith('order_data'):
                    process_order_file(file_path, conn)
                elif file_name.startswith('order_states'):
                    process_order_states_file(file_path, conn)

                # File deletion logic is inside the processing functions.
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")

        print(f"Done with the search... next search in 10 seconds.")
        time.sleep(10)  # Check for new files every 10 seconds



if __name__ == "__main__":
    # Directory to watch for CSV files
    directory_to_watch = "/Users/rasagna/ingest_data_from_file_with_python/files/watch_directory_uploads"

    # Connect to DuckDB (replace with your database file path if persistent)
    conn = duckdb.connect('/Users/rasagna/ingest_data_from_file_with_python/data/dynamic_state_ingestion_database.duckdb')

    # Start watching the directory
    watch_directory(directory_to_watch, conn)

    # Close the connection when done
    conn.close()
