
Core Simulation Engine for the AI-Powered Tolling System.

This module contains the classes and logic for running a discrete-event simulation
of a toll plaza. It is designed to be the foundation for testing dynamic pricing,
resource allocation, and other AI-driven strategies.

Key Components:
- Vehicle: Represents a vehicle with specific characteristics (type, axles, speed).
- TollBooth: Manages a queue of vehicles and processes them.
- TollPlaza: Orchestrates the entire simulation, including vehicle generation
and data collection.
'''
import random
import math
import heapq
from enum import Enum
from collections import deque

# --- Configuration ---
SIMULATION_DURATION_HOURS = 1  # Simulate for 1 hour
VEHICLES_PER_HOUR_AVG = 200  # Average number of vehicles arriving per hour
NUM_TOLL_BOOTHS = 3

# --- Vehicle Definitions ---
class VehicleType(Enum):
    CAR = "Car"
    BUS = "Bus"
    TRUCK = "Truck"

class Vehicle:
    '''Represents a single vehicle in the simulation.'''
    def __init__(self, vehicle_id, arrival_time):
        self.id = vehicle_id
        self.arrival_time = arrival_time
        self.departure_time = 0

        # Assign vehicle type and properties
        self.vehicle_type = random.choices(
            [VehicleType.CAR, VehicleType.BUS, VehicleType.TRUCK],
            weights=[0.6, 0.15, 0.25],  # 60% Cars, 15% Buses, 25% Trucks
            k=1
        )[0]

        if self.vehicle_type == VehicleType.CAR:
            self.axles = 2
        elif self.vehicle_type == VehicleType.BUS:
            self.axles = random.choice([2, 3])
        else:  # Truck
            self.axles = random.randint(3, 6)

        self.speed = random.uniform(40, 80)  # Speed in km/h

    def __repr__(self):
        return f"Vehicle({self.id}, {self.vehicle_type.name}, {self.axles} axles)"

# --- Toll Infrastructure Classes ---
class TollBooth:
    '''Represents a single toll booth with a vehicle queue.'''
    def __init__(self, booth_id):
        self.id = booth_id
        self.vehicle_queue = deque()
        self.is_busy = False
        self.current_vehicle = None

    def add_vehicle(self, vehicle):
        '''Adds a vehicle to the end of the queue.'''
        self.vehicle_queue.append(vehicle)

    def get_queue_length(self):
        '''Returns the number of vehicles waiting in the queue.'''
        return len(self.vehicle_queue)

class TollPlaza:
    '''Manages the toll booths and orchestrates the simulation.'''
    def __init__(self, num_booths, vehicles_per_hour):
        print("Initializing Toll Plaza Simulation...")
        self.booths = [TollBooth(i) for i in range(num_booths)]
        self.simulation_time = 0
        self.event_queue = []
        self.vehicles_processed = 0
        self.total_wait_time = 0
        self.next_vehicle_id = 0

        # Average time between arrivals in seconds (for Poisson process)
        self.avg_arrival_interval = 3600 / vehicles_per_hour

    def _get_next_arrival_time(self):
        '''Calculates next vehicle arrival time using a Poisson process.'''
        return self.simulation_time + random.expovariate(1.0 / self.avg_arrival_interval)

    def _schedule_vehicle_arrival(self):
        '''Schedules the next vehicle to arrive.'''
        arrival_time = self._get_next_arrival_time()
        heapq.heappush(self.event_queue, (arrival_time, "VEHICLE_ARRIVAL"))

    def _get_shortest_queue_booth(self):
        '''Finds the toll booth with the shortest queue.'''
        return min(self.booths, key=lambda booth: booth.get_queue_length())

    def run_simulation(self, duration_seconds):
        '''Runs the main discrete-event simulation loop.'''
        print(f"Starting simulation for {duration_seconds / 3600:.1f} hours.")
        self._schedule_vehicle_arrival() # Schedule the very first vehicle

        while self.event_queue and self.simulation_time < duration_seconds:
            # Get the next event from the priority queue
            event_time, event_type, *event_data = heapq.heappop(self.event_queue)
            self.simulation_time = event_time

            if event_type == "VEHICLE_ARRIVAL":
                self.next_vehicle_id += 1
                vehicle = Vehicle(self.next_vehicle_id, self.simulation_time)
                print(f"TIME: {self.simulation_time:.2f}s - {vehicle} ARRIVED")

                # Route vehicle to the booth with the shortest queue
                booth = self._get_shortest_queue_booth()
                booth.add_vehicle(vehicle)
                print(f"    >> Routed to Booth {booth.id} (Queue: {booth.get_queue_length()})")

                # If the booth was idle, schedule its processing immediately
                if not booth.is_busy:
                    heapq.heappush(self.event_queue, (self.simulation_time, "START_PROCESSING", booth.id))

                # Schedule the next vehicle to arrive
                self._schedule_vehicle_arrival()

            elif event_type == "START_PROCESSING":
                booth_id = event_data[0]
                booth = self.booths[booth_id]
                if booth.vehicle_queue:
                    booth.is_busy = True
                    booth.current_vehicle = booth.vehicle_queue.popleft()
                    
                    # Processing time depends on number of axles (e.g., 2 seconds per axle)
                    processing_time = booth.current_vehicle.axles * 2
                    finish_time = self.simulation_time + processing_time
                    heapq.heappush(self.event_queue, (finish_time, "FINISH_PROCESSING", booth.id))

            elif event_type == "FINISH_PROCESSING":
                booth_id = event_data[0]
                booth = self.booths[booth_id]
                processed_vehicle = booth.current_vehicle

                processed_vehicle.departure_time = self.simulation_time
                wait_time = processed_vehicle.departure_time - processed_vehicle.arrival_time
                self.total_wait_time += wait_time
                self.vehicles_processed += 1
                print(f"TIME: {self.simulation_time:.2f}s - Vehicle {processed_vehicle.id} FINISHED at Booth {booth.id}. Wait time: {wait_time:.2f}s")

                booth.is_busy = False
                booth.current_vehicle = None

                # If there are more vehicles in the queue, start processing the next one
                if booth.vehicle_queue:
                    heapq.heappush(self.event_queue, (self.simulation_time, "START_PROCESSING", booth.id))

        print("\n--- Simulation Ended ---")
        self.print_statistics()

    def print_statistics(self):
        '''Prints a summary of the simulation results.'''
        print("\n--- Simulation Statistics ---")
        print(f"Total vehicles processed: {self.vehicles_processed}")
        if self.vehicles_processed > 0:
            avg_wait_time = self.total_wait_time / self.vehicles_processed
            print(f"Average wait time: {avg_wait_time:.2f} seconds")
        else:
            print("No vehicles were processed.")
        print(f"Remaining vehicles in queues: {sum(b.get_queue_length() for b in self.booths)}")

# --- Main Execution ---
if __name__ == "__main__":
    simulation_duration_seconds = SIMULATION_DURATION_HOURS * 3600
    toll_plaza = TollPlaza(num_booths=NUM_TOLL_BOOTHS, vehicles_per_hour=VEHICLES_PER_HOUR_AVG)
    toll_plaza.run_simulation(simulation_duration_seconds)
