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

    # Приводим столбец с ценой (7-я колонка) к числовому формату
    price_column = df.columns[6]
    print(f"Обрабатывается столбец с ценой: {price_column}")

    # Преобразуем цену, оставляя числовое значение, и сохраняем исходный столбец
    df[f"{price_column}_число"] = df[price_column].str.replace(r"[^\d.,]", "", regex=True).str.replace(",", ".").str.strip(".")
    df[f"{price_column}_число"] = pd.to_numeric(df[f"{price_column}_число"], errors='coerce')
    print(f"Первые значения в обработанном столбце цены:\n{df[[price_column, f'{price_column}_число']].head()}")

    # Вычисляем индекс изменений (продажи)
    def calculate_sold(row):
        sold = 0
        for i in range(1, len(quantity_columns)):
            prev = row[quantity_columns[i - 1]]
            curr = row[quantity_columns[i]]
            if curr < prev:
                sold += prev - curr
        return sold

    df["Индекс изменений"] = df.apply(calculate_sold, axis=1)
    print(f"Первые значения индекса изменений:\n{df[['Индекс изменений']].head()}")

    # Вычисляем новый индекс "Заработали" = "Индекс изменений" * минимальная цена
    df["Заработали"] = df["Индекс изменений"] * df[f"{price_column}_число"]
    print(f"Первые значения индекса 'Заработали':\n{df[['Индекс изменений', f'{price_column}_число', 'Заработали']].head()}")

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