import os
import time
import duckdb
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from collections import deque
from file_processors import process_order_file, process_order_states_file


class CSVFileHandler(FileSystemEventHandler):
    def __init__(self, conn):
        self.conn = conn
        self.file_queue = deque()

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            file_path = event.src_path
            file_name = os.path.basename(file_path)

            # Enqueue files with priority for order_data files
            if file_name.startswith('order_data'):
                self.file_queue.appendleft(file_path)  # High priority
            elif file_name.startswith('order_states'):
                self.file_queue.append(file_path)  # Low priority

            # Process files in the queue
            self.process_files()

    def process_files(self):
        while self.file_queue:
            file_path = self.file_queue.popleft()

            try:
                if os.path.basename(file_path).startswith('order_data'):
                    process_order_file(file_path, self.conn)
                elif os.path.basename(file_path).startswith('order_states'):
                    process_order_states_file(file_path, self.conn)

                # File deletion logic is in the processing functions.
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")

def watch_with_watchdog(directory, conn):
    print(f"Starting watchdog observer for directory: {directory}")
    event_handler = CSVFileHandler(conn)
    observer = Observer()
    observer.schedule(event_handler, directory, recursive=False)

    try:
        observer.start()
        while True:
            time.sleep(1)  # Keep the script running
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


if __name__ == "__main__":
    # Directory to watch for CSV files
    directory_to_watch = "/Users/rasagna/ingest_data_from_file_with_python/files/watchdog_uploads"

    # Connect to DuckDB (replace with your database file path if persistent)
    conn = duckdb.connect('/Users/rasagna/ingest_data_from_file_with_python/data/dynamic_state_ingestion_database.duckdb')

    # Start the watchdog observer
    watch_with_watchdog(directory_to_watch, conn)

    # Close the connection when done
    conn.close()
