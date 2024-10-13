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
    c.execute('INSERT INTO data (key, value) VALUES (?, ?)', (key, value))
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
    insert_data(1, "UserA")  # Shard 2
    insert_data(2, "UserB")  # Shard 3
    insert_data(3, "UserC")  # Shard 1
    insert_data(4, "UserD")  # Shard 2

    print(get_data(1))  # Should return "UserA" from Shard 2
    print(get_data(2))  # Should return "UserB" from Shard 3
    print(get_data(3))  # Should return "UserC" from Shard 1
    print(get_data(4))  # Should return "UserD" from Shard 2
