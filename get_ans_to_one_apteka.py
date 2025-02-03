import pandas as pd
import os

input_folder = "Datasets/pre_ans"
output_folder = "Datasets/pre_ans_sorted"

os.makedirs(output_folder, exist_ok=True)

def process_file(file_path):
    print(f"Обрабатывается файл: {file_path}")
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"Ошибка при загрузке файла {file_path}: {e}")
        return None

    print(df.head())
    print(f"Столбцы в файле: {df.columns}")

    # Определяем столбцы с количеством
    quantity_columns = [col for col in df.columns if col.startswith("Количество")]
    print(f"Найдены столбцы с количеством: {quantity_columns}")

    if len(quantity_columns) < 2:
        print(f"Ошибка: недостаточно столбцов с количеством в файле {file_path}")
        return None

    # Приводим данные количественных столбцов к числовому типу
    df[quantity_columns] = df[quantity_columns].apply(pd.to_numeric, errors='coerce')

    # Приводим столбцы с ценами к числовому типу
    # Предполагаем, что у вас есть колонка "Медианная цена"
    price_column = "Медианная цена"
    if price_column not in df.columns:
        print(f"Ошибка: столбец '{price_column}' отсутствует в файле {file_path}")
        return None
    print(f"Обрабатывается столбец с ценой: {price_column}")

    # Преобразуем столбец с ценой в числовой тип
    df[price_column] = df[price_column].astype(str).str.replace(',', '.')
    df[price_column] = df[price_column].str.extract(r'(\d+\.?\d*)', expand=False)
    df[price_column] = pd.to_numeric(df[price_column], errors='coerce')

    print(f"Первые значения в столбце цены:\n{df[[price_column]].head()}")

    # Заполняем пропуски в столбце цен нулями
    df[price_column] = df[price_column].fillna(0)

    # Вычисляем индекс изменений (продажи)
    def calculate_sold(row):
        sold = 0
        for i in range(1, len(quantity_columns)):
            prev = row[quantity_columns[i - 1]]
            curr = row[quantity_columns[i]]
            if pd.notnull(prev) and pd.notnull(curr):
                if curr < prev:
                    sold += prev - curr
        return sold

    df["Индекс изменений"] = df.apply(calculate_sold, axis=1)
    print(f"Первые значения индекса изменений:\n{df[['Индекс изменений']].head()}")

    # Вычисляем новый индекс "Заработали" = "Индекс изменений" * "Медианная цена"
    df["Заработали"] = df["Индекс изменений"] * df[price_column]
    print(f"Первые значения индекса 'Заработали':\n{df[['Индекс изменений', price_column, 'Заработали']].head()}")

    # Фильтруем строки, в которых "Индекс изменений" > 0
    df_filtered = df[df["Индекс изменений"] > 0]
    print(f"Количество строк с продажами: {len(df_filtered)}")

    # Сортируем данные по новому индексу "Заработали" в порядке убывания
    df_sorted = df_filtered.sort_values(by="Заработали", ascending=False)
    print(f"Первые строки после сортировки:\n{df_sorted.head()}")

    return df_sorted

# Обработка всех файлов в input_folder
for file_name in os.listdir(input_folder):
    if file_name.endswith(".xls") or file_name.endswith(".xlsx"):
        file_path = os.path.join(input_folder, file_name)
        sorted_table = process_file(file_path)
        if sorted_table is not None:
            output_file_path = os.path.join(output_folder, f"sorted_{file_name}")
            sorted_table.to_excel(output_file_path, index=False)
            print(f"Результат сохранён в файл: {output_file_path}")
        else:
            print(f"Файл {file_name} пропущен из-за ошибок.")
