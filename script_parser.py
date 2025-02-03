import subprocess
import sys
import platform

# Функция для чтения данных из файла
def read_data(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
        return [line.strip().split() for line in lines if line.strip()]
    except FileNotFoundError:
        print(f'Ошибка: файл {file_path} не найден.')
        sys.exit(1)

# Путь к файлу с данными
file_path = 'data.txt'  # Укажите путь к вашему файлу

# Чтение данных
data = read_data(file_path)

# Запуск main.py для каждой строки данных в новом окне
for entry in data:
    if len(entry) == 2:  # Убедимся, что строка содержит два элемента
        name, url = entry
        print(f'Запуск main.py с аргументами: {name} {url}')

        # Определяем операционную систему
        if platform.system() == 'Windows':
            # Для Windows
            command = f'powershell -NoExit -Command "python main.py \'{name}\' \'{url}\'; Write-Host \\"Нажмите любую клавишу для выхода...\\"; $Host.UI.RawUI.ReadKey() | Out-Null"'
            subprocess.Popen(command, shell=True)
        else:
            # Для Linux
            subprocess.Popen(['gnome-terminal', '--', 'python', 'main.py', name, url])  # Для Linux
            # Для macOS
            # subprocess.Popen(['osascript', '-e', f'tell application "Terminal" to do script "python main.py {name} {url}"'])
    else:
        print(f'Пропущена строка: {entry} - неверный формат')