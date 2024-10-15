import tkinter as tk
import tkinter.messagebox as messagebox
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


def insert_data_gui():
    try:
        key = int(key_entry.get())
        value = value_entry.get()
        insert_data(key, value)
        result_text.set(f"Inserted key={key}, value={value}")
    except ValueError:
        messagebox.showerror("Error", "Invalid input. Key must be an integer.")


def get_data_gui():
    try:
        key = int(key_entry.get())
        value = get_data(key)
        result_text.set(f"Value for key {key}: {value}")
    except ValueError:
        messagebox.showerror("Error", "Invalid input. Key must be an integer.")


def delete_data_gui():
    try:
        key = int(key_entry.get())
        delete_data(key)
        result_text.set(f"Deleted key {key}")
    except ValueError:
        messagebox.showerror("Error", "Invalid input. Key must be an integer.")


def show_all_data_gui():
    show_all_shard_data()
    result_text.set("Shard data displayed in console.")


if __name__ == '__main__':
    create_db('shard_1')
    create_db('shard_2')
    create_db('shard_3')

    window = tk.Tk()
    window.title("Sharded Database")

    key_label = tk.Label(window, text="Key:")
    key_label.grid(row=0, column=0)
    key_entry = tk.Entry(window)
    key_entry.grid(row=0, column=1)

    value_label = tk.Label(window, text="Value:")
    value_label.grid(row=1, column=0)
    value_entry = tk.Entry(window)
    value_entry.grid(row=1, column=1)

    insert_button = tk.Button(window, text="Insert", command=insert_data_gui)
    insert_button.grid(row=2, column=0)

    get_button = tk.Button(window, text="Get", command=get_data_gui)
    get_button.grid(row=2, column=1)

    delete_button = tk.Button(window, text="Delete", command=delete_data_gui)
    delete_button.grid(row=3, column=0)

    show_button = tk.Button(window, text="Show All Data", command=show_all_data_gui)
    show_button.grid(row=3, column=1)

    result_text = tk.StringVar()
    result_label = tk.Label(window, textvariable=result_text)
    result_label.grid(row=4, columnspan=2)

    window.mainloop()
