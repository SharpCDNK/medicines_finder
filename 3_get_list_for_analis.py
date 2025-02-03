import os
import pandas as pd
import logging
import re
import multiprocessing

# Путь к папке с конкурентами
base_path = "Datasets/diff_comp"
output_path = "Datasets/list_for_analis"

# Проверяем, существует ли папка для сохранения результатов, если нет — создаем её
if not os.path.exists(output_path):
    os.makedirs(output_path)

# Путь к файлу логов
log_file_path = os.path.join(output_path, 'log.txt')

# Настройка логирования
logging.basicConfig(
    filename=log_file_path,  # Путь к файлу логов
    level=logging.INFO,      # Уровень логирования
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Функция для получения индекса из имени файла
def get_index_from_filename(file_name):
    match = re.search(r'diff_parsed_data_(\d+)_', file_name)
    return int(match.group(1)) if match else None

# Функция для обработки одного конкурента
def process_competitor(competitor_name):
    folder_path = os.path.join(base_path, competitor_name)
    # Список для хранения данных
    all_data = []
    unique_records = set()  # Множество для проверки уникальности строк по первым 6 колонкам

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

                # Фильтруем данные по item_type
                excluded_types = ["БАД", "Косметика", "Питание", "Прочее", "зубные пасты и проч.","Продукты питания"]
                df = df[~df['item_type'].isin(excluded_types)]

                # Добавляем данные в общий список, проверяя на уникальность
                for _, row in df[required_columns].iterrows():
                    record_key = tuple(row[col] for col in required_columns[:6])  # Ключ — первые 6 колонок
                    if record_key not in unique_records:
                        unique_records.add(record_key)  # Добавляем ключ в множество
                        all_data.append(row.to_dict())  # Добавляем запись в общий список
                        log_message = f"Добавлена уникальная запись из файла {file_name}: {row.to_dict()}"
                        logging.info(log_message)
                    else:
                        log_message = f"Пропущена дублирующая запись из файла {file_name}: {row.to_dict()}"
                        logging.info(log_message)
            else:
                log_message = f"Отсутствуют необходимые колонки в файле {file_name}"
                logging.warning(log_message)
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

# Основной цикл для обработки всех папок конкурентов с использованием multiprocessing
# Получаем список конкурентов
competitors = [competitor for competitor in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, competitor))]

# Запускаем процессы для каждого конкурента
with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
    pool.map(process_competitor, competitors)
