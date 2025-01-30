import os
import time
from datetime import datetime
from utils import *  # Убедитесь, что эта библиотека реализована
from parser import *  # Убедитесь, что эта библиотека реализована

# Флаг для остановки планировщика
stop_scheduler = False

# Функция для выполнения задачи
def job(cur_apteka, cur_apteka_link):
    print(cur_apteka, ' ', cur_apteka_link)
    if cur_apteka and cur_apteka_link:
        print(f"Запуск парсера для {cur_apteka} в {datetime.now().strftime('%H:%M')}")
        # Сохраняем данные
        save_directory = os.path.join(os.getcwd(), 'Datasets', 'competitors', cur_apteka)
        os.makedirs(save_directory, exist_ok=True)
        get_parser_data(cur_apteka_link, save_directory)

# Планировщик
def run_schedule(cur_apteka, cur_apteka_link):
    global stop_scheduler
    # Времена выполнения задач
    task_times = ["09:20", "11:20", "13:20", "15:20", "17:20", "19:20", "21:20", "23:20"]

    # Словарь для хранения времени последнего выполнения
    last_run_time = None

    while not stop_scheduler:
        current_time = datetime.now().strftime('%H:%M')

        # Проверяем, совпадает ли текущее время с любым из запланированных
        if current_time in task_times and current_time != last_run_time:
            print(f"Время совпало: {current_time}, запускаем задачу!")
            job(cur_apteka, cur_apteka_link)
            last_run_time = current_time  # Обновляем время последнего выполнения
            # Ждем одну минуту, чтобы избежать повторного запуска в ту же минуту
            time.sleep(60)
        else:
            os.system('clear')
            print("Аптека: ", cur_apteka, ", ссылка: ", cur_apteka_link)
            print({current_time}, "- текущее время.")
            time.sleep(4)

        # Проверяем, если уже следующий день, сбрасываем last_run_time
        if current_time == "00:00":
            last_run_time = None

# Функция для запуска процесса планирования
def start_schedule(cur_apteka, cur_apteka_link):
    global stop_scheduler
    print("Запуск планировщика...")
    try:
        run_schedule(cur_apteka, cur_apteka_link)
    except KeyboardInterrupt:
        print("Планировщик остановлен.")

# Функция для остановки планировщика
def stop_schedule():
    global stop_scheduler
    stop_scheduler = True
    print("Планировщик завершен.")