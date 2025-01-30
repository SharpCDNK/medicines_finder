import os
from schedule import start_schedule, stop_schedule,job  # Импортируем только нужные функции
import sys

# Запрос имени аптеки и ссылки


def main(cur_apteka,cur_apteka_link):
    # Запускаем планировщик в основном потоке
    start_schedule(cur_apteka, cur_apteka_link)


if __name__ == "__main__":
    # Проверяем, что передано 2 аргумента
    if len(sys.argv) != 3:
        print("Ошибка: требуется 2 аргумента (аптека и ссылка).")
        sys.exit(1)

    # Получаем аргументы из командной строки
    cur_apteka = sys.argv[1]
    cur_apteka_link = sys.argv[2]

    # Вызываем основную функцию
    main(cur_apteka, cur_apteka_link)