import os
import time
from datetime import datetime, timedelta
from utils import *  # Убедитесь, что эта библиотека реализована
from parser import *  # Убедитесь, что эта библиотека реализована

# Флаг для остановки планировщика
stop_scheduler = False

# Функция для выполнения задачи
def job(cur_apteka, cur_apteka_link):
    print(cur_apteka, ' ', cur_apteka_link)
    if cur_apteka and cur_apteka_link:
        print(f"Запуск парсера для {cur_apteka} в {datetime.now().strftime('%H:%M:%S')}")
        # Сохраняем данные
        save_directory = os.path.join(os.getcwd(), 'Datasets', 'competitors', cur_apteka)
        os.makedirs(save_directory, exist_ok=True)
        get_parser_data(cur_apteka_link, save_directory)

# Функция для вычисления времени до следующего запуска
def time_until_next_run(current_time, task_times):
    now = datetime.strptime(current_time, '%H:%M')
    for task_time in task_times:
        task_datetime = datetime.strptime(task_time, '%H:%M')
        if task_datetime > now:
            return task_datetime - now
    # Если текущий день закончился, берем первое время на следующий день
    first_task_time = datetime.strptime(task_times[0], '%H:%M')
    next_day_task_time = first_task_time + timedelta(days=1)
    return next_day_task_time - now

# Планировщик
def run_schedule(cur_apteka, cur_apteka_link):
    global stop_scheduler
    # Времена выполнения задач
    task_times = ["08:20", "10:20", "12:20", "14:20", "16:20", "18:20", "20:20", "22:20", "23:30"]

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
            os.system('cls' if os.name == 'nt' else 'clear')  # Очистка экрана
            print(f"=== Планировщик задач ===")
            print(f"Текущая аптека: {cur_apteka}")
            print(f"Ссылка: {cur_apteka_link}")
            print(f"Текущее время: {current_time}")

            # Вычисляем время до следующего запуска
            time_left = time_until_next_run(current_time, task_times)
            print(f"До следующего запуска осталось: {time_left}")

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