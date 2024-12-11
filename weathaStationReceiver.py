import sys
import json
import os
import psycopg2

# Database connection details
DB_NAME = os.getenv("POSTGRES_DB", "database")
DB_USER = os.getenv("POSTGRES_USER", "bene")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DB_HOST = "localhost"
DB_PORT = 5432

# Cache for the last packet
cache = None

def create_table_if_not_exists(conn):
    """Create the klimaData table if it does not exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS klimaData (
                time TIMESTAMPTZ NOT NULL DEFAULT now(),
                channel INT NOT NULL,
                id INT NOT NULL,
                temperature REAL,
                humidity REAL,
                PRIMARY KEY (time, channel)
            );
        """)
        conn.commit()
        print("Table klimaData is ready.", file=sys.stderr)

def insert_data(conn, channel, sensor_id, temperature, humidity):
    """Insert data into the klimaData table."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO klimaData (time, channel, id, temperature, humidity)
                VALUES (now(), %s, %s, %s, %s)
                ON CONFLICT (time, channel) DO NOTHING;
            """, (channel, sensor_id, temperature, humidity))
            conn.commit()
    except psycopg2.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        conn.rollback()

def is_duplicate(packet):
    """Check if the packet is a duplicate based on content."""
    global cache
    return packet == cache

def main():
    global cache

    # Connect to the database
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        create_table_if_not_exists(conn)
    except Exception as e:
        print(f"Failed to connect to the database: {e}", file=sys.stderr)
        sys.exit(1)

    for line in sys.stdin:
        try:
            # Parse the JSON message from the input line
            msg = json.loads(line.strip())

            # Check if the model is "AmbientWeather-WH31E"
            if msg.get("model") == "AmbientWeather-WH31E":
                # Extract the required fields
                sensor_id = msg.get("id")
                humidity = msg.get("humidity")
                temperature = msg.get("temperature_C")
                channel = msg.get("channel")

                # Skip records with missing data
                if channel is None or sensor_id is None or temperature is None or humidity is None:
                    print("Skipping record with missing data", file=sys.stderr)
                    continue

                # Construct the packet content for comparison
                packet = (channel, sensor_id, temperature, humidity)

                if is_duplicate(packet):
                    print(f"Duplicate packet ignored: id: {sensor_id}, channel: {channel}, "
                          f"humidity: {humidity}%, temperature: {temperature}°C")
                else:
                    # Update the cache and insert into the database
                    cache = packet
                    print(f"New packet: id: {sensor_id}, channel: {channel}, "
                          f"humidity: {humidity}%, temperature: {temperature}°C")
                    insert_data(conn, channel, sensor_id, temperature, humidity)

        except json.JSONDecodeError:
            print("Invalid JSON format", file=sys.stderr)
        except Exception as e:
            print(f"Error processing message: {e}", file=sys.stderr)
            conn.rollback()

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    main()
