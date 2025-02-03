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
    print(df.columns)

    quantity_columns = [col for col in df.columns if col.startswith("Количество")]
    print(f"Найдены столбцы с количеством: {quantity_columns}")

    if len(quantity_columns) < 2:
        print(f"Ошибка: недостаточно столбцов с количеством в файле {file_path}")
        return None

    df[quantity_columns] = df[quantity_columns].apply(pd.to_numeric, errors='coerce')

    def calculate_sold(row):
        sold = 0
        for i in range(1, len(quantity_columns)):
            prev = row[quantity_columns[i - 1]]
            curr = row[quantity_columns[i]]
            if curr < prev:
                sold += prev - curr
        return sold

    df["Индекс изменений"] = df.apply(calculate_sold, axis=1)
    df_filtered = df[df["Индекс изменений"] > 0]
    print(f"Количество строк с продажами: {len(df_filtered)}")
    df_sorted = df_filtered.sort_values(by="Индекс изменений", ascending=False)
    print(df_sorted.head())
    return df_sorted

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