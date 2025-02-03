import os
import pandas as pd
import logging
import re

# Настройка логирования
logging.basicConfig(
    filename='Datasets/list_for_analis/log.txt',  # Путь к файлу логов
    level=logging.INFO,  # Уровень логирования
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Путь к папке с конкурентами
base_path = "Datasets/diff_comp"
output_path = "Datasets/list_for_analis"

# Проверяем, существует ли папка для сохранения результатов, если нет — создаем
if not os.path.exists(output_path):
    os.makedirs(output_path)

# Функция для получения индекса из имени файла
def get_index_from_filename(file_name):
    match = re.search(r'diff_parsed_data_(\d+)_', file_name)
    return int(match.group(1)) if match else None

# Функция для обработки одного конкурента
def process_competitor(folder_path, competitor_name):
    # Список для хранения данных
    all_data = []

    # Получаем список файлов и сортируем по индексу
    files = [f for f in os.listdir(folder_path) if f.endswith(".xlsx") or f.endswith(".xls")]
    files.sort(key=get_index_from_filename)

    # Проходимся по всем файлам в папке конкурента
    for file_name in files:
        file_path = os.path.join(folder_path, file_name)

        try:
            # Читаем Excel файл
            df = pd.read_excel(file_path, engine="openpyxl")

            # Проверяем, что в таблице есть нужные колонки (можно уточнить структуру)
            required_columns = ["name", "item_type", "item_form", "prescription", "manufacturer", "country", "price"]
            if all(column in df.columns for column in required_columns):
                # Очищаем данные от лишних пробелов в строках
                df[required_columns] = df[required_columns].apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

                # Добавляем данные в общий список
                for _, row in df[required_columns].iterrows():
                    log_message = f"Обрабатывается запись из файла {file_name}: {row.to_dict()}"
                    logging.info(log_message)
                    all_data.append(row.to_dict())
        except Exception as e:
            error_message = f"Ошибка при обработке файла {file_name}: {e}"
            logging.error(error_message)

    # Если данные найдены, объединяем их и сохраняем в Excel
    if all_data:
        combined_df = pd.DataFrame(all_data)

        # Создаем папку для конкурента, если она еще не создана
        competitor_output_path = os.path.join(output_path, competitor_name)
        if not os.path.exists(competitor_output_path):
            os.makedirs(competitor_output_path)

        # Сохраняем результат в файл Excel
        output_file = os.path.join(competitor_output_path, f"{competitor_name}_list_for_analis.xlsx")
        combined_df.to_excel(output_file, index=False, engine="openpyxl")
        success_message = f"Данные для конкурента '{competitor_name}' сохранены в {output_file}"
        print(success_message)
        logging.info(success_message)
    else:
        no_data_message = f"Нет данных для конкурента '{competitor_name}'"
        print(no_data_message)
        logging.warning(no_data_message)

# Основной цикл для обработки всех папок конкурентов
for competitor in os.listdir(base_path):
    competitor_path = os.path.join(base_path, competitor)

    # Проверяем, что это папка
    if os.path.isdir(competitor_path):
        start_message = f"Обработка данных для конкурента: {competitor}"
        print(start_message)
        logging.info(start_message)
        process_competitor(competitor_path, competitor)