import sqlite3
import os

def create_connection(db_file):
    """
    Create a database connection to the SQLite database specified by db_file.
    
    Args:
        db_file (str): The path to the SQLite database file.
        
    Returns:
        sqlite3.Connection: Connection object or None if connection fails.
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to database: {db_file}")
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
    
    return conn

def create_table(conn):
    """
    Create a table in the database if it does not exist.
    
    Args:
        conn (sqlite3.Connection): The connection object to the database.
    """
    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ipaddress TEXT,
                timestamp TEXT,
                method TEXT,
                path TEXT,
                protocol TEXT,
                status_code INTEGER,
                bytes_sent INTEGER,
                referrer TEXT,
                user_agent TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_line TEXT,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        conn.commit()
        print("Table 'logs' created successfully.")
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")


def insert_log_entries(conn, entries):
    """
    Inserts multiple log entries into the logs table.

    Args:
        conn: SQLite connection object.
        entries (list): A list of parsed log entry dictionaries.
    """
    if not entries:
        return  # Avoid inserting if list is empty

    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO logs (ipaddress, timestamp, method, path, protocol, status_code, bytes_sent, referrer, user_agent)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', [
        (
            entry['ipaddress'],
            entry['timestamp'],
            entry['method'],
            entry['path'],
            entry['protocol'],
            entry['status_code'],
            entry['bytes_sent'],
            entry['referrer'],
            entry['user_agent']
        )
        for entry in entries
    ])
    print(f"Inserted {len(entries)} log entries into the database.")

def insert_error_entries(conn, error_entries):
    """
    Insert multiple error entries into the errors table.
    Args:
        conn (sqlite3.Connection): The connection object to the database.
        error_entries (list): A list of error entries, each a dictionary with 'raw_line' and 'error_message'.
    """
    if not error_entries:
        return  # Avoid inserting if list is empty

    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO errors (raw_line, error_message)
        VALUES (?, ?)
    ''', [
        (error.get('raw_line', str(error)), error.get('error_message', '')) if isinstance(error, dict) else (str(error), '')
        for error in error_entries
    ])
    print(f"Inserted {len(error_entries)} error entries into the database.")
    
def is_db_up_to_date(db_path, log_file_path):
    try:
        db_mtime = os.path.getmtime(db_path)
        log_mtime = os.path.getmtime(log_file_path)
        return db_mtime >= log_mtime
    except Exception:
        return False

def is_db_empty(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM logs")
        count = cursor.fetchone()[0]
        return count == 0
    except Exception as e:
        return True  # Assume empty if there's an error
