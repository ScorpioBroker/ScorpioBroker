#!/usr/bin/env python3
"""
NGSI-LD Entity Generator Script (Deterministic Version)

This script generates 50,000 NGSI-LD entities with deterministic properties and relationships,
then sends them to an NGSI-LD broker via the entityOperations/create endpoint in batches of 1,000.
All values are generated using repeatable patterns instead of random data.
"""

import json
import requests
from datetime import datetime, timezone
import time
from typing import List, Dict, Any
import uuid

# Configuration
BROKER_URL = "http://localhost:9090"  # Change to your NGSI-LD broker URL
TOTAL_ENTITIES = 50000
BATCH_SIZE = 1000
ENTITY_TYPE = "SensorDevice"

# Deterministic data pools
PROPERTY_NAMES = ["temperature", "humidity", "pressure", "luminosity", "voltage"]
RELATIONSHIP_NAMES = ["belongsTo", "connectedTo", "locatedAt", "managedBy", "partOf"]
LOCATIONS = ["Building-A", "Building-B", "Building-C", "Room-101", "Room-102"]
UNITS = ["Celsius", "Percent", "Pascal", "Lux", "Volt"]

# Base values for properties (will be modified deterministically)
BASE_VALUES = {
    "temperature": 20.0,
    "humidity": 50.0,
    "pressure": 1013.25,
    "luminosity": 500.0,
    "voltage": 12.0,
}

# Base coordinates for location (Madrid, Spain area)
BASE_LAT = 40.4168
BASE_LON = -3.7038
# Grid parameters for spreading entities across area (roughly 10km x 10km)
LAT_RANGE = 0.09  # ~10km north-south
LON_RANGE = 0.13  # ~10km east-west


def generate_deterministic_string(index: int, length: int = 8) -> str:
    """Generate a deterministic string based on index."""
    # Create a repeatable pattern using the index
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    result = ""
    temp_index = index

    for i in range(length):
        result += chars[temp_index % len(chars)]
        temp_index = temp_index // len(chars) + 1

    return result


def generate_location_geoproperty(entity_index: int) -> Dict[str, Any]:
    """Generate a deterministic NGSI-LD location geoproperty."""
    # Create a grid pattern for coordinates
    grid_size = int(TOTAL_ENTITIES**0.5) + 1  # Square grid that fits all entities

    row = entity_index // grid_size
    col = entity_index % grid_size

    # Calculate coordinates within the defined area
    lat_offset = (row / grid_size) * LAT_RANGE - (LAT_RANGE / 2)
    lon_offset = (col / grid_size) * LON_RANGE - (LON_RANGE / 2)

    latitude = BASE_LAT + lat_offset
    longitude = BASE_LON + lon_offset

    return {
        "type": "GeoProperty",
        "value": {
            "type": "Point",
            "coordinates": [round(longitude, 6), round(latitude, 6)],
        },
    }
    """Generate a deterministic NGSI-LD property."""
    base_value = BASE_VALUES.get(name, 50.0)

    # Create deterministic variation based on entity index and property name
    variation_factor = (entity_index % 100) / 100.0  # 0.0 to 0.99
    name_hash = sum(ord(c) for c in name) % 10  # Simple hash of property name

    # Apply deterministic variation
    value = base_value + (variation_factor * 50) + (name_hash * 5)

    return {"type": "Property", "value": round(value, 2)}


def generate_relationship(name: str, entity_index: int) -> Dict[str, Any]:
    """Generate a deterministic NGSI-LD relationship."""
    # Select location based on entity index and relationship name
    location_index = (entity_index + sum(ord(c) for c in name)) % len(LOCATIONS)
    location = LOCATIONS[location_index]

    # Generate deterministic suffix
    suffix = generate_deterministic_string(entity_index + location_index, 6)

    return {"type": "Relationship", "object": f"urn:ngsi-ld:{location}:{suffix}"}


def generate_entity(entity_index: int) -> Dict[str, Any]:
    """Generate a single NGSI-LD entity with deterministic properties and relationships."""
    # Generate deterministic entity ID
    entity_suffix = generate_deterministic_string(entity_index, 12)
    entity_id = f"urn:ngsi-ld:{ENTITY_TYPE}:{entity_suffix}"

    entity = {"id": entity_id, "type": ENTITY_TYPE}

    # Add 3 deterministic properties
    # Select properties based on entity index
    property_indices = [
        entity_index % len(PROPERTY_NAMES),
        (entity_index + 1) % len(PROPERTY_NAMES),
        (entity_index + 2) % len(PROPERTY_NAMES),
    ]

    # Ensure we don't have duplicate properties
    property_indices = list(set(property_indices))
    while len(property_indices) < 3:
        property_indices.append((max(property_indices) + 1) % len(PROPERTY_NAMES))
    if entity_index % 2 == 0:
      for i in range(3):
        prop_name = PROPERTY_NAMES[property_indices[i]]
        entity[prop_name] = generate_property(prop_name, entity_index)

    # Add 2 deterministic relationships
    relationship_indices = [
        entity_index % len(RELATIONSHIP_NAMES),
        (entity_index + 1) % len(RELATIONSHIP_NAMES),
    ]

    # Ensure we don't have duplicate relationships
    if relationship_indices[0] == relationship_indices[1]:
        relationship_indices[1] = (relationship_indices[1] + 1) % len(
            RELATIONSHIP_NAMES
        )

    for i in range(2):
        rel_name = RELATIONSHIP_NAMES[relationship_indices[i]]
        entity[rel_name] = generate_relationship(rel_name, entity_index)

    # Add deterministic location geoproperty
    entity["location"] = generate_location_geoproperty(entity_index)

    return entity


def create_batch(batch_entities: List[Dict[str, Any]]) -> bool:
    """Send a batch of entities to the NGSI-LD broker."""
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    url = f"{BROKER_URL}/ngsi-ld/v1/entityOperations/upsert"

    try:
        response = requests.post(url, json=batch_entities, headers=headers)

        if response.status_code == 201 or response.status_code == 204:
            print(f"✓ Successfully created batch of {len(batch_entities)} entities")
            return True
        else:
            print(f"✗ Failed to create batch. Status: {response.status_code}")
            print(f"Response: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"✗ Error sending batch: {e}")
        return False


def main():
    """Main function to generate and send all entities."""
    print(f"Starting deterministic NGSI-LD entity generation...")
    print(f"Target: {TOTAL_ENTITIES} entities of type '{ENTITY_TYPE}'")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Broker URL: {BROKER_URL}")
    print("-" * 50)

    total_batches = TOTAL_ENTITIES // BATCH_SIZE
    successful_batches = 0
    failed_batches = 0

    start_time = time.time()
    for  i in range(1):
      print(10000 - i)
      for batch_num in range(total_batches):
        print(f"\nProcessing batch {batch_num + 1}/{total_batches}")

        # Generate batch of entities
        batch_entities = []
        for i in range(BATCH_SIZE):
            # Calculate global entity index
            entity_index = batch_num * BATCH_SIZE + i
            entity = generate_entity(entity_index)
            batch_entities.append(entity)

        # Send batch to broker
        if create_batch(batch_entities):
            successful_batches += 1
        else:
            failed_batches += 1

        # Small delay between batches to avoid overwhelming the broker
        time.sleep(0.1)

    end_time = time.time()
    duration = end_time - start_time

    # Summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Total batches processed: {total_batches}")
    print(f"Successful batches: {successful_batches}")
    print(f"Failed batches: {failed_batches}")
    print(f"Total entities created: {successful_batches * BATCH_SIZE}")
    print(f"Total time: {duration:.2f} seconds")
    print(f"Average time per batch: {duration/total_batches:.2f} seconds")

    if failed_batches > 0:
        print(
            f"\n⚠️  {failed_batches} batches failed. Check broker connectivity and logs."
        )
    else:
        print(f"\n🎉 All {TOTAL_ENTITIES} entities created successfully!")


def generate_property(name: str, entity_index: int) -> Dict[str, Any]:
    """Generate a deterministic NGSI-LD property."""
    base_value = BASE_VALUES.get(name, 50.0)

    # Create deterministic variation based on entity index and property name
    variation_factor = (entity_index % 100) / 100.0  # 0.0 to 0.99
    name_hash = sum(ord(c) for c in name) % 10  # Simple hash of property name

    # Apply deterministic variation
    value = base_value + (variation_factor * 50) + (name_hash * 5)

    return {"type": "Property", "value": round(value, 2)}


def preview_entities(count: int = 5):
    """Preview the first few entities that would be generated."""
    print("Preview of entities that will be generated:")
    print("=" * 50)

    for i in range(count):
        entity = generate_entity(i)
        print(f"\nEntity {i + 1}:")
        print(json.dumps(entity, indent=2))


if __name__ == "__main__":
    # Show a preview of what will be generated
    preview_entities(3)

    # Ask user if they want to proceed
    response = input("\nDo you want to proceed with entity creation? (y/n): ")
    if response.lower() != "y":
        print("Entity creation cancelled.")
        exit()

    # Verify broker connectivity before starting
    try:
        response = requests.get(f"{BROKER_URL}/version")
        if response.status_code == 200:
            print(f"✓ Broker connectivity confirmed: {BROKER_URL}")
        else:
            print(f"⚠️  Broker returned status {response.status_code}")
    except requests.exceptions.RequestException:
        print(
            f"⚠️  Cannot reach broker at {BROKER_URL}. Please check the URL and ensure the broker is running."
        )
        print("Proceeding anyway - you may see connection errors...")

    main()
