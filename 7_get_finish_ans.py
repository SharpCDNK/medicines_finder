import os
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from functools import partial

# Пути к папкам и файлам
source_folder = 'Datasets/ans_sorted_comp'
destination_folder = 'Datasets/Finish'
destination_file = os.path.join(destination_folder, 'final_result.xlsx')

# Создаём папку назначения, если её ещё нет
os.makedirs(destination_folder, exist_ok=True)

# Определяем нужные колонки
key_columns = ['name', 'item_type', 'item_form', 'prescription', 'manufacturer', 'country']
required_columns = key_columns + [
    'Цена Мин', 'Цена Макс', 'Цена Медиум', 'Индекс Изменений', 'Заработали'
]


# Функция для обработки одного файла
def process_file(filename, source_folder):
    file_path = os.path.join(source_folder, filename)
    print(f"Обработка файла: {filename}")
    try:
        # Считываем данные из файла
        data = pd.read_excel(file_path)
    except Exception as e:
        print(f"Ошибка при загрузке файла {file_path}: {e}")
        return pd.DataFrame()  # Возвращаем пустой DataFrame в случае ошибки

    # Приводим названия колонок к нижнему регистру и убираем пробелы по краям
    data.columns = data.columns.str.strip().str.lower()

    # Словарь для переименования колонок
    rename_columns = {
        'цена min': 'Цена Мин',
        'цена мин': 'Цена Мин',
        'цена max': 'Цена Макс',
        'цена макс': 'Цена Макс',
        'медианная цена': 'Цена Медиум',
        'медианнаяцена': 'Цена Медиум',
        'индекс изменений': 'Индекс Изменений',
        'индексизменений': 'Индекс Изменений',
        'заработали': 'Заработали'
        # Добавьте другие соответствия, если требуется
    }

    # Переименовываем колонки согласно словарю
    data.rename(columns=rename_columns, inplace=True)

    # Проверяем наличие необходимых колонок
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        print(f"В файле {filename} отсутствуют колонки: {', '.join(missing_columns)}. Пропускаем этот файл.")
        return pd.DataFrame()

    # Получаем идентификатор файла
    file_identifier = '_'.join(filename.replace('.xlsx', '').split('_')[-2:])

    # Добавляем информацию об источнике
    data['Источники'] = (
            f"{file_identifier}, цена мин = " + data['Цена Мин'].astype(str) +
            ", цена макс = " + data['Цена Макс'].astype(str) +
            ", цена медиум = " + data['Цена Медиум'].astype(str) +
            ", индекс изменений = " + data['Индекс Изменений'].astype(str) +
            ", заработали = " + data['Заработали'].astype(str)
    )

    # Добавляем колонку "Аптеки"
    data['Аптеки'] = 1

    return data


if __name__ == "__main__":
    # Получаем список файлов для обработки
    file_list = [filename for filename in os.listdir(source_folder) if filename.endswith(('.xlsx', '.xls'))]

    # Используем multiprocessing для параллельной обработки файлов
    with ProcessPoolExecutor() as executor:
        func = partial(process_file, source_folder=source_folder)
        # Собираем результаты в список
        data_frames = list(executor.map(func, file_list))

    # Исключаем пустые DataFrame перед объединением
    data_frames = [df for df in data_frames if not df.empty]

    # Объединяем все полученные DataFrame
    final_df = pd.concat(data_frames, ignore_index=True)

    # Если итоговый DataFrame пуст, завершаем выполнение
    if final_df.empty:
        print("Нет данных для обработки.")
    else:
        # Группируем данные по ключевым колонкам
        aggregation_functions = {
            'Цена Мин': 'min',
            'Цена Макс': 'max',
            'Цена Медиум': 'mean',
            'Индекс Изменений': 'sum',
            'Заработали': 'sum',
            'Источники': '; '.join,
            'Аптеки': 'sum'
        }

        final_df = final_df.groupby(key_columns, as_index=False).agg(aggregation_functions)

        # Сохраняем итоговый DataFrame в Excel-файл
        final_df.to_excel(destination_file, index=False)
        print(f"Финальный результат сохранён в файл: {destination_file}")
