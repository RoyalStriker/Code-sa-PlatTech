import sqlite3
import os

db_directory = 'db_files'

if not os.path.exists(db_directory):
    os.makedirs(db_directory)

def create_db(db_name):
    """Create a SQLite database with a 'data' table."""
    db_path = os.path.join(db_directory, f'{db_name}.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS data (key INTEGER PRIMARY KEY, value TEXT)''')
    conn.commit()
    return conn

shard_1 = create_db('shard_1')
shard_2 = create_db('shard_2')
shard_3 = create_db('shard_3')

def get_shard_for_key(key):
    """Simple sharding function using modulo to determine the shard."""
    shard_number = key % 3 + 1
    return f'shard_{shard_number}'

def insert_data(key, value):
    """Insert data into the correct shard based on the key."""
    shard = get_shard_for_key(key)
    db_path = os.path.join(db_directory, f'{shard}.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO data (key, value) VALUES (?, ?)', (key, value))
    conn.commit()
    conn.close()
    print(f'Inserted key={key} into {shard}')

def get_data(key):
    """Retrieve data from the correct shard."""
    shard = get_shard_for_key(key)
    db_path = os.path.join(db_directory, f'{shard}.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('SELECT value FROM data WHERE key=?', (key,))
    result = c.fetchone()
    conn.close()
    if result:
        return result[0]
    else:
        return "Data not found"

if __name__ == '__main__':
    while True:
        try:
            choice = input("Do you want to (i)nsert, (r)etrieve, or (d)elete data? (i/r/d): ").lower()
            if choice == 'i':
                key = int(input("Enter the key (integer): "))
                value = input("Enter the value: ")
                insert_data(key, value)
                print(f'Retrieved data for key={key}: {get_data(key)}')
            elif choice == 'r':
                key = int(input("Enter the key (integer) to retrieve: "))
                print(f'Retrieved data for key={key}: {get_data(key)}')
            elif choice == 'd':
                key = int(input("Enter the key (integer) to delete: "))
                delete_data(key)
            else:
                print("Invalid choice. Please enter 'i' to insert, 'r' to retrieve, or 'd' to delete data.")
        
        except ValueError:
            print("Invalid input. Please enter a valid integer for the key.")

        cont = input("Continue? (y/n): ").lower()
        if cont != 'y':
            print("Exiting...")
            break
