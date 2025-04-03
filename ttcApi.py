import requests
import psycopg2
from datetime import datetime

# -------------------------
# Configuration Parameters
# -------------------------
base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
package_id = "ttc-bus-delay-data"

# PostgreSQL connection configuration – update these with your own credentials
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "ttcDelays"
DB_USER = "postgres"
DB_PASSWORD = "admin@123"

# -------------------------
# Database Functions
# -------------------------
def create_table(conn):
    """
    Creates the ttcDelays table with individual columns if it doesn't already exist.
    The table will have columns for:
      - day (TEXT)
      - record_id (INTEGER) -- from _id in the record
      - date (TIMESTAMP)
      - time (TEXT)
      - bound (TEXT)
      - route (TEXT)
      - min_gap (INTEGER)
      - station (TEXT)
      - vehicle (TEXT)
      - incident (TEXT)
      - min_delay (INTEGER)
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ttcDelays (
                id SERIAL PRIMARY KEY,
                day TEXT,
                record_id INTEGER,
                date TIMESTAMP,
                time TEXT,
                bound TEXT,
                route TEXT,
                min_gap INTEGER,
                station TEXT,
                vehicle TEXT,
                incident TEXT,
                min_delay INTEGER
            );
        """)
        conn.commit()

def insert_record(conn, record):
    """
    Extracts fields from a record and inserts them into the ttcDelays table.
    Assumes the record is a dict with keys:
      "Day", "_id", "Date", "Time", "Bound", "Route", "Min Gap", "Station", "Vehicle", "Incident", "Min Delay"
    """
    day = record.get("Day")
    record_id = record.get("_id")
    date_str = record.get("Date")
    try:
        date_val = datetime.fromisoformat(date_str) if date_str else None
    except Exception as e:
        print(f"Error parsing date '{date_str}': {e}")
        date_val = None
    time_val = record.get("Time")
    bound = record.get("Bound")
    route = record.get("Route")
    try:
        min_gap = int(record.get("Min Gap")) if record.get("Min Gap") is not None else None
    except ValueError:
        min_gap = None
    station = record.get("Station")
    vehicle = record.get("Vehicle")
    incident = record.get("Incident")
    try:
        min_delay = int(record.get("Min Delay")) if record.get("Min Delay") is not None else None
    except ValueError:
        min_delay = None

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ttcDelays 
            (day, record_id, date, time, bound, route, min_gap, station, vehicle, incident, min_delay)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (day, record_id, date_val, time_val, bound, route, min_gap, station, vehicle, incident, min_delay))
    conn.commit()

# -------------------------
# Data Fetching Functions
# -------------------------
def fetch_data_for_resource(resource_id):
    """
    Fetches data from the datastore_search API endpoint using pagination.
    """
    limit = 1000
    offset = 0
    total = None
    records = []

    while True:
        search_url = f"{base_url}/api/3/action/datastore_search"
        params = {
            "id": resource_id,
            "limit": limit,
            "offset": offset
        }
        print(f"Fetching records from offset {offset}...")
        response = requests.get(search_url, params=params)
        if response.status_code != 200:
            print(f"Error fetching data: HTTP {response.status_code}")
            break

        data = response.json()
        if not data.get("success"):
            print("API call was not successful:", data)
            break

        result = data.get("result", {})
        if total is None:
            total = result.get("total", 0)
            print(f"Total records to fetch: {total}")

        batch = result.get("records", [])
        if not batch:
            break

        records.extend(batch)
        offset += limit
        if offset >= total:
            break

    return records

# -------------------------
# Main Execution Flow
# -------------------------
def main():
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Connected to PostgreSQL database.")
    except Exception as e:
        print("Database connection failed:", e)
        return

    # Create the ttcDelays table if it doesn't exist
    create_table(conn)

    # Fetch package metadata from the TTC API
    package_url = f"{base_url}/api/3/action/package_show"
    params = {"id": package_id}
    package_response = requests.get(package_url, params=params)
    if package_response.status_code != 200:
        print("Error fetching package metadata:", package_response.status_code)
        return
    package_data = package_response.json()

    # Process each resource in the package
    resources = package_data.get("result", {}).get("resources", [])
    for resource in resources:
        if resource.get("datastore_active"):
            resource_id = resource.get("id")
            print(f"Processing datastore resource: {resource_id}")
            records = fetch_data_for_resource(resource_id)
            print(f"Inserting {len(records)} records into ttcDelays table...")
            for rec in records:
                insert_record(conn, rec)
        else:
            print(f"Skipping non-datastore resource: {resource.get('id')}")

    # Close the database connection
    conn.close()
    print("Data fetching and storing completed.")

if __name__ == "__main__":
    main()
