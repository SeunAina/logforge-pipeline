# Report Generation
from database import create_connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def summarize_log(db_file):
    """
    Summarizes the logs stored in the SQLite database.
    
    Args:
        db_file (str): Path to the SQLite database file.
        
    Returns:
        dict: A dictionary with log levels as keys and their counts as values.
    """

    conn = create_connection(db_file)
    if not conn:
        logger.error("Failed to connect to the database for summarization")
        return

    cursor = conn.cursor()
    
    # Select top endpoints
    cursor.execute('''
        SELECT path, COUNT(*) as count
        FROM logs
        GROUP BY path
        ORDER BY count DESC
        LIMIT 10
    ''')
    top_endpoints = cursor.fetchall()
    
    # Status code distribution
    cursor.execute('''
        SELECT status_code, COUNT(*) as count
        FROM logs
        GROUP BY status_code
    ''')
    status_code_distribution = cursor.fetchall()
    
    # Top client IPs
    cursor.execute('''
        SELECT ipaddress, COUNT(*) as count
        FROM logs
        GROUP BY ipaddress
        ORDER BY count DESC
        LIMIT 10
    ''')
    top_client_ips = cursor.fetchall()

    summary = {
        'top_endpoints': [{'path': path, 'count': count} for path, count in top_endpoints],
        'status_code_distribution': [{'status_code': status_code, 'count': count} for status_code, count in status_code_distribution],
        'top_client_ips': [{'ipaddress': ipaddress, 'count': count} for ipaddress, count in top_client_ips]
    }

    conn.close()
    logger.info("Log summary generated successfully")
    return summary