import sqlite3
import os

db_directory = 'db_files'

if not os.path.exists(db_directory):
    os.makedirs(db_directory)

def create_db(db_name):
    """Create a SQLite database with a 'data' table."""
    db_path = os.path.join(db_directory, f'{db_name}.db')
    with sqlite3.connect(db_path) as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS data (key INTEGER PRIMARY KEY, value TEXT)''')
        conn.commit()

create_db('shard_1')
create_db('shard_2')
create_db('shard_3')

def get_shard_for_key(key):
    """Simple sharding function using modulo to determine the shard."""
    shard_number = key % 3 + 1
    return f'shard_{shard_number}'

def insert_data(key, value):
    """Insert data into the correct shard based on the key."""
    with sqlite3.connect(os.path.join(db_directory, f'{get_shard_for_key(key)}.db')) as conn:
        c = conn.cursor()
        c.execute('INSERT OR REPLACE INTO data (key, value) VALUES (?, ?)', (key, value))
        conn.commit()
    print(f'Inserted key={key} into {get_shard_for_key(key)}')

def get_data(key):
    """Retrieve data from the correct shard."""
    with sqlite3.connect(os.path.join(db_directory, f'{get_shard_for_key(key)}.db')) as conn:
        c = conn.cursor()
        c.execute('SELECT value FROM data WHERE key=?', (key,))
        result = c.fetchone()
    if result:
        return result[0]
    else:
        return "Data not found"

def delete_data(key):
    """Delete data from the correct shard."""
    shard = get_shard_for_key(key)
    with sqlite3.connect(os.path.join(db_directory, f'{shard}.db')) as conn:
        c = conn.cursor()
        c.execute('DELETE FROM data WHERE key=?', (key,))
        conn.commit()
        print(f'Deleted key={key} from {shard}')

def show_shard_data(shard_name):
    """Prints the data contained within a specific shard."""
    db_path = os.path.join(db_directory, f'{shard_name}.db')
    try:
        with sqlite3.connect(db_path) as conn:
            c = conn.cursor()
            c.execute("SELECT * FROM data")
            rows = c.fetchall()

            if rows:
                print(f"\nData in {shard_name}:")
                for row in rows:
                    print(f"  Key: {row[0]}, Value: {row[1]}")
            else:
                print(f"\n{shard_name} is empty.")
    except sqlite3.OperationalError as e:
        print(f"Error accessing {shard_name}: {e}")


def show_all_shard_data():
    """Prints the data in all shards."""
    for i in range(1, 4):
        show_shard_data(f"shard_{i}")


if __name__ == '__main__':
    while True:
        try:
            choice = input("Do you want to (i)nsert, (r)etrieve, (d)elete, or (s)how data? (i/r/d/s): ").lower()
            if choice == 'i':
                key = int(input("Enter the key (integer): "))
                value = input("Enter the value: ")
                insert_data(key, value)
                print(f'Retrieved data for key={key}: {get_data(key)}') # Verify insert
            elif choice == 'r':
                key = int(input("Enter the key (integer) to retrieve: "))
                print(f'Retrieved data for key={key}: {get_data(key)}')
            elif choice == 'd':
                key = int(input("Enter the key (integer) to delete: "))
                delete_data(key)
            elif choice == 's':
                show_all_shard_data()
            else:
                print("Invalid choice. Please enter 'i', 'r', 'd', or 's'.")

        except ValueError:
            print("Invalid input. Please enter a valid integer for the key.")

        cont = input("Continue? (y/n): ").lower()
        if cont != 'y':
            print("Exiting...")
            break
