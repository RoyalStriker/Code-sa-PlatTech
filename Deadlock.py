import threading

lock1 = threading.Lock()
lock2 = threading.Lock()

def thread1_routine():
    print("Thread 1: Attempting to acquire lock 1...")
    lock1.acquire()
    print("Thread 1: Acquired lock 1, now trying to acquire lock 2...")
    lock2.acquire()
    print("Thread 1: Acquired lock 2.")

    # Release locks after work
    lock2.release()
    lock1.release()

def thread2_routine():
    print("Thread 2: Attempting to acquire lock 2...")
    lock2.acquire()
    print("Thread 2: Acquired lock 2, now trying to acquire lock 1...")
    lock1.acquire()
    print("Thread 2: Acquired lock 1.")

    # Release locks after work
    lock1.release()
    lock2.release()

t1 = threading.Thread(target=thread1_routine)
t2 = threading.Thread(target=thread2_routine)

t1.start()
t2.start()

t1.join()
t2.join()
