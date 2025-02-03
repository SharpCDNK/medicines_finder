import pandas as pd
import os

# Определяем пути к папкам
input_folder = "Datasets/pre_ans_sorted"
output_folder = "Datasets/ans_sorted_comp"

# Создаём выходную папку, если её ещё нет
os.makedirs(output_folder, exist_ok=True)

# Определяем нужные колонки
required_columns = [
    'name', 'item_type', 'item_form', 'prescription', 'manufacturer', 'country',
    'Цена min', 'Цена max', 'Медианная цена', 'Индекс изменений', 'Заработали'
]

# Функция для обработки одного файла
def process_file(file_path, output_folder):
    print(f"Обрабатывается файл: {file_path}")
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"Ошибка при загрузке файла {file_path}: {e}")
        return

    # Оставляем только нужные колонки
    df_filtered = df[required_columns]

    # Формируем путь для сохранения файла
    output_file_path = os.path.join(output_folder, os.path.basename(file_path))

    # Сохраняем отфильтрованный DataFrame в новый файл
    df_filtered.to_excel(output_file_path, index=False)
    print(f"Результат сохранён в файл: {output_file_path}")

# Обрабатываем все файлы в папке input_folder
for file_name in os.listdir(input_folder):
    if file_name.endswith(".xls") or file_name.endswith(".xlsx"):
        file_path = os.path.join(input_folder, file_name)
        process_file(file_path, output_folder)
