import json
from collections import OrderedDict

SUBSCRIPTION_FILE = 'config/888224317991706685.json'
SHIPS_FILE = 'config/ships.json'

def migrate_subscriptions():
    """
    Migrates ShipGroup filters in the subscription file from using
    Type IDs to using Group IDs.
    """
    print("Starting migration...")

    # 1. Load the ship type-to-group mapping
    try:
        with open(SHIPS_FILE, 'r') as f:
            # The ships.json is a map of "type_id_str": group_id
            type_to_group_map = json.load(f)
            # Convert string keys to int for easier lookup
            type_to_group_map = {int(k): v for k, v in type_to_group_map.items()}
        print(f"Successfully loaded {len(type_to_group_map)} ship type mappings from {SHIPS_FILE}")
    except FileNotFoundError:
        print(f"ERROR: Ship mapping file not found at {SHIPS_FILE}")
        return
    except json.JSONDecodeError:
        print(f"ERROR: Could not decode JSON from {SHIPS_FILE}")
        return

    # 2. Load the subscription data
    try:
        with open(SUBSCRIPTION_FILE, 'r') as f:
            subscriptions = json.load(f)
        print(f"Successfully loaded {len(subscriptions)} subscriptions from {SUBSCRIPTION_FILE}")
    except FileNotFoundError:
        print(f"ERROR: Subscription file not found at {SUBSCRIPTION_FILE}")
        return
    except json.JSONDecodeError:
        print(f"ERROR: Could not decode JSON from {SUBSCRIPTION_FILE}")
        return

    # Memoization cache to avoid redundant conversions
    conversion_cache = {}
    migrations_count = 0

    def migrate_node(node):
        nonlocal migrations_count
        if not isinstance(node, dict):
            return

        for key, value in node.items():
            if key == "ShipGroup" and isinstance(value, list):
                # Create a tuple from the list to use as a cache key
                original_list_key = tuple(sorted(value))

                if original_list_key in conversion_cache:
                    # Use cached result
                    node[key] = conversion_cache[original_list_key]
                    print(f"  - Found cached conversion for list: {list(original_list_key)}")
                else:
                    # Perform new conversion
                    print(f"  - Performing new conversion for list: {list(original_list_key)}")

                    # Use an OrderedDict to preserve order while getting unique group IDs
                    group_ids = OrderedDict()
                    for type_id in value:
                        group_id = type_to_group_map.get(type_id)
                        if group_id:
                            group_ids[group_id] = None
                        else:
                            # If it's not in the map, it might already be a group ID. Keep it.
                            group_ids[type_id] = None
                            print(f"    - Warning: TypeID {type_id} not found in ships.json. Assuming it's already a GroupID.")

                    new_group_id_list = list(group_ids.keys())
                    node[key] = new_group_id_list

                    # Cache the result
                    conversion_cache[original_list_key] = new_group_id_list
                    migrations_count += 1
                    print(f"    -> Converted to: {new_group_id_list}")

            elif isinstance(value, dict):
                migrate_node(value)
            elif isinstance(value, list):
                for item in value:
                    migrate_node(item)

    # 3. Iterate through all subscriptions and migrate their filters
    print("\nScanning subscriptions for ShipGroup filters...")
    for sub in subscriptions:
        if "filter" in sub:
            migrate_node(sub["filter"])

    # 4. Write the migrated data back to the file
    try:
        with open(SUBSCRIPTION_FILE, 'w') as f:
            json.dump(subscriptions, f, indent=2)
        print(f"\nMigration complete. {migrations_count} unique ShipGroup lists were converted.")
        print(f"The file {SUBSCRIPTION_FILE} has been updated in place.")
    except IOError as e:
        print(f"ERROR: Could not write to file {SUBSCRIPTION_FILE}. Error: {e}")


if __name__ == "__main__":
    migrate_subscriptions()
