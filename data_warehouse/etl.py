import duckdb
import json
import os
from datetime import datetime

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define paths relative to the script directory
db_path = os.path.join(script_dir, 'toll_analytics.db')
simulation_events_path = os.path.join(script_dir, '..', 'simulation_events.jsonl') # Assuming it's in the project root

def run_etl():
    """
    Reads simulation events from a JSONL file, transforms them, and loads them
    into the DuckDB data warehouse.
    """
    con = duckdb.connect(database=db_path, read_only=False)

    # Ensure dimension tables have default/lookup values if needed
    # For simplicity, we'll assume dimensions are pre-populated or handled on-the-fly
    # In a real scenario, you'd have more robust dimension management.

    processed_transactions = []

    if not os.path.exists(simulation_events_path):
        print(f"Simulation events file not found at: {simulation_events_path}")
        return

    with open(simulation_events_path, 'r') as f:
        for line in f:
            try:
                event = json.loads(line.strip())
                # Assuming 'transaction_event' is the type of event we care about
                if event.get('event_type') == 'transaction_event':
                    # --- Transformation Logic (Example) ---
                    # This is where you map your simulation event data to the star schema.
                    # You'll need to extract and format data for each dimension and fact.

                    # Example: Extracting data for fact_transactions
                    transaction_data = {
                        'transaction_key': event.get('transaction_id'), # Use a unique ID from your simulation
                        'toll_fee': event.get('toll_fee'),
                        'travel_distance_km': event.get('distance'),
                        'travel_time_seconds': event.get('travel_time'),
                        'queue_length_at_transaction': event.get('queue_length'),
                        # Foreign keys - these would typically come from lookups or pre-populated dimensions
                        'toll_plaza_key': None, # Placeholder
                        'vehicle_key': None,    # Placeholder
                        'date_key': None,       # Placeholder
                        'payment_method_key': None # Placeholder
                    }

                    # Example: Populating dim_date (simplified)
                    event_timestamp = datetime.fromisoformat(event.get('timestamp'))
                    date_key = int(event_timestamp.strftime('%Y%m%d'))
                    transaction_data['date_key'] = date_key

                    # Insert into dim_date if not exists (simplified for example)
                    con.execute(f"INSERT OR IGNORE INTO dim_date (date_key, full_date, day, month, year, quarter, day_of_week) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                date_key, event_timestamp.date(), event_timestamp.day, event_timestamp.month, event_timestamp.year, (event_timestamp.month - 1) // 3 + 1, event_timestamp.isoweekday())

                    # Example: Populating dim_vehicle (simplified)
                    vehicle_id = event.get('vehicle_id')
                    vehicle_key = hash(vehicle_id) # Simple hash for key, use proper surrogate key generation
                    transaction_data['vehicle_key'] = vehicle_key
                    con.execute(f"INSERT OR IGNORE INTO dim_vehicle (vehicle_key, vehicle_id, license_plate, vehicle_type, axle_count) VALUES (?, ?, ?, ?, ?)",
                                vehicle_key, vehicle_id, event.get('license_plate'), event.get('vehicle_type'), event.get('axle_count'))

                    # Example: Populating dim_toll_plaza (simplified)
                    toll_plaza_id = event.get('toll_plaza_id')
                    toll_plaza_key = hash(toll_plaza_id) # Simple hash for key
                    transaction_data['toll_plaza_key'] = toll_plaza_key
                    con.execute(f"INSERT OR IGNORE INTO dim_toll_plaza (toll_plaza_key, toll_plaza_id, name) VALUES (?, ?, ?)",
                                toll_plaza_key, toll_plaza_id, event.get('toll_plaza_name'))

                    # Example: Populating dim_payment_method (simplified)
                    payment_method_name = event.get('payment_method')
                    payment_method_key = hash(payment_method_name) # Simple hash for key
                    transaction_data['payment_method_key'] = payment_method_key
                    con.execute(f"INSERT OR IGNORE INTO dim_payment_method (payment_method_key, method_name) VALUES (?, ?)",
                                payment_method_key, payment_method_name)

                    processed_transactions.append(transaction_data)

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e} in line: {line.strip()}")
            except Exception as e:
                print(f"An error occurred during ETL processing: {e} for event: {event}")

    # --- Load into fact_transactions ---
    if processed_transactions:
        # Convert list of dicts to a format suitable for DuckDB insertion
        # For large datasets, consider using DuckDB's COPY FROM or Pandas DataFrame insertion
        for trans in processed_transactions:
            try:
                con.execute(
                    "INSERT INTO fact_transactions (
                        transaction_key, toll_plaza_key, vehicle_key, date_key, payment_method_key,
                        toll_fee, travel_distance_km, travel_time_seconds, queue_length_at_transaction
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    trans['transaction_key'], trans['toll_plaza_key'], trans['vehicle_key'],
                    trans['date_key'], trans['payment_method_key'], trans['toll_fee'],
                    trans['travel_distance_km'], trans['travel_time_seconds'],
                    trans['queue_length_at_transaction']
                )
            except duckdb.ConstraintException as e:
                print(f"Skipping duplicate transaction or constraint violation: {e}")
            except Exception as e:
                print(f"Error inserting transaction: {e} for data: {trans}")

    con.close()
    print(f"ETL process completed. Processed {len(processed_transactions)} transactions.")

if __name__ == "__main__":
    run_etl()
