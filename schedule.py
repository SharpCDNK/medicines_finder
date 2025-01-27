import os
import time
from datetime import datetime
import threading
from parser import *  # Ensure this library is implemented

# Flag for stopping the thread
stop_thread = False

# Function to execute the job
def job(cur_apteka,cur_apteka_link):

    if cur_apteka and cur_apteka_link:
        print(f"Запуск парсера для {cur_apteka} в {datetime.now().strftime('%H:%M')}")
        # Save data
        save_directory = os.path.join(os.getcwd(), 'Datasets', 'competitors', cur_apteka)
        os.makedirs(save_directory, exist_ok=True)
        get_parser_data(cur_apteka_link, save_directory)

# Scheduler function
def run_schedule():
    global stop_thread

    # Task times for execution
    task_times = ["09:20", "11:20", "14:20", "15:45", "20:20", "22:20"]

    while not stop_thread:
        current_time = datetime.now().strftime('%H:%M')

        # Check if current time matches any scheduled time
        if current_time in task_times:
            print(f"Время совпало: {current_time}, запускаем задачу!")
            job()
            # Wait one minute to avoid re-running in the same minute
            time.sleep(60)
        else:
            # Sleep for one second to prevent CPU overload
            time.sleep(1)

# Function to stop the scheduler
def stop_schedule():
    global stop_thread
    stop_thread = True

# Start the scheduler in a separate thread
def start_thread():
    thread = threading.Thread(target=run_schedule)
    thread.daemon = True  # Thread will stop when the program exits
    thread.start()
    return thread

# Function to start the scheduling process
def start_schedule():
    print("Запуск планировщика...")
    thread = start_thread()

    input("Нажмите Enter, чтобы завершить выполнение...\n")

    stop_schedule()
    thread.join()  # Wait for the thread to finish

