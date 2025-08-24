-- Dimension Tables

CREATE TABLE IF NOT EXISTS dim_toll_plaza (
    toll_plaza_key INTEGER PRIMARY KEY,
    toll_plaza_id VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    location VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    commercial_operation_date DATE
);

CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_key INTEGER PRIMARY KEY,
    vehicle_id VARCHAR(255) NOT NULL,
    license_plate VARCHAR(255),
    vehicle_type VARCHAR(50),
    axle_count INTEGER
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    quarter INTEGER,
    day_of_week INTEGER
);

CREATE TABLE IF NOT EXISTS dim_payment_method (
    payment_method_key INTEGER PRIMARY KEY,
    method_name VARCHAR(255) NOT NULL
);

-- Fact Table

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_key INTEGER PRIMARY KEY,
    toll_plaza_key INTEGER,
    vehicle_key INTEGER,
    date_key INTEGER,
    payment_method_key INTEGER,
    toll_fee DECIMAL(10, 2),
    travel_distance_km DECIMAL(10, 2),
    travel_time_seconds INTEGER,
    queue_length_at_transaction INTEGER,
    FOREIGN KEY (toll_plaza_key) REFERENCES dim_toll_plaza(toll_plaza_key),
    FOREIGN KEY (vehicle_key) REFERENCES dim_vehicle(vehicle_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (payment_method_key) REFERENCES dim_payment_method(payment_method_key)
);
