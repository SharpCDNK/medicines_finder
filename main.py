import os
from schedule import start_schedule, stop_schedule,job  # Импортируем только нужные функции

# Запрос имени аптеки и ссылки
cur_apteka = input("Введите название аптеки: ")
cur_apteka_link = input("Введите ссылку на аптеку: ")

def main():
    # Запускаем планировщик в основном потоке
    job(cur_apteka, cur_apteka_link)
    #start_schedule(cur_apteka, cur_apteka_link)

if __name__ == "__main__":
    main()