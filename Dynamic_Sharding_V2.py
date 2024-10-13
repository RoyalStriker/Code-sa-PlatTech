import sqlite3
import os

db_directory = './shards'

if not os.path.exists(db_directory):
    os.makedirs(db_directory)

def get_shard_for_key(key):
    return f'shard_{key % 3 + 1}'  # Assuming 3 shards

def insert_data(key, value):
    shard = get_shard_for_key(key)
    db_path = os.path.join(db_directory, f'{shard}.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    c.execute('CREATE TABLE IF NOT EXISTS data (key INTEGER PRIMARY KEY, value TEXT)')
    
    try:
        c.execute('INSERT INTO data (key, value) VALUES (?, ?)', (key, value))
        conn.commit()
        print(f'Inserted key={key} into {shard}')
    except sqlite3.IntegrityError:
        print(f'Failed to insert data for key={key}: Key already exists in {shard}')
    
    conn.close()

def get_data(key):
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

def delete_data(key):
    shard = get_shard_for_key(key)
    db_path = os.path.join(db_directory, f'{shard}.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute('DELETE FROM data WHERE key=?', (key,))
    conn.commit()
    conn.close()

    print(f'Deleted key={key} from {shard}')

def show_data_in_shard(shard):
    db_path = os.path.join(db_directory, f'{shard}.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    c.execute('SELECT key, value FROM data')
    results = c.fetchall()
    
    conn.close()
    
    if results:
        print(f"Data in {shard}:")
        for key, value in results:
            print(f"  Key: {key}, Value: {value}")
    else:
        print(f"No data found in {shard}.")

def show_all_data():
    for i in range(1, 4):  # Assuming there are 3 shards
        shard_name = f'shard_{i}'
        show_data_in_shard(shard_name)

if __name__ == '__main__':
    while True:
        try:
            choice = input("Do you want to (i)nsert, (r)etrieve, (d)elete, or (s)how data? (i/r/d/s): ").lower()
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
            elif choice == 's':
                show_all_data()  # This line will call the function to show all data
            else:
                print("Invalid choice. Please enter 'i' to insert, 'r' to retrieve, 'd' to delete, or 's' to show data.")
        
        except ValueError:
            print("Invalid input. Please enter a valid integer for the key.")

        cont = input("Continue? (y/n): ").lower()
        if cont != 'y':
            print("Exiting...")
            break
