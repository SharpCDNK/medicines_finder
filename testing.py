import pandas as pd
import os
from concurrent.futures import ProcessPoolExecutor
from functools import partial


def process_file(file_name, input_folder, output_folder):
    file_path = os.path.join(input_folder, file_name)
    print(f"Обрабатывается файл: {file_path}")
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"Ошибка при загрузке файла {file_path}: {e}")
        return

    quantity_columns = [col for col in df.columns if col.startswith("Количество")]
    if len(quantity_columns) < 2:
        print(f"Ошибка: недостаточно столбцов с количеством в файле {file_path}")
        return

    df[quantity_columns] = df[quantity_columns].apply(pd.to_numeric, errors='coerce')
    price_column = "Медианная цена"
    if price_column not in df.columns:
        print(f"Ошибка: столбец '{price_column}' отсутствует в файле {file_path}")
        return

    df[price_column] = df[price_column].astype(str).str.replace(',', '.')
    df[price_column] = df[price_column].str.extract(r'(\d+\.?\d*)', expand=False)
    df[price_column] = pd.to_numeric(df[price_column], errors='coerce').fillna(0)

    def calculate_sold(row):
        sold = 0
        for i in range(1, len(quantity_columns)):
            prev, curr = row[quantity_columns[i - 1]], row[quantity_columns[i]]
            if pd.notnull(prev) and pd.notnull(curr) and curr < prev:
                sold += prev - curr
        return sold

    df["Индекс изменений"] = df.apply(calculate_sold, axis=1)
    df["Заработали"] = df["Индекс изменений"] * df[price_column]

    def calculate_kps(row):
        segments = []
        temp_segment = []
        prev_val = None

        for col in quantity_columns:
            curr_val = row[col]
            if prev_val is not None and curr_val > prev_val:
                if temp_segment:
                    segments.append(temp_segment)
                temp_segment = []
            temp_segment.append(curr_val)
            prev_val = curr_val
        if temp_segment:
            segments.append(temp_segment)

        num_segments = len(segments)
        segment_sums = [sum(seg[:-1]) - sum(seg[1:]) for seg in segments if len(seg) > 1]
        avg_kps = sum(segment_sums) / num_segments if num_segments else 0

        return f"{avg_kps:.2f} ({num_segments}, {sum(segment_sums)}/{num_segments if num_segments else 1})"

    df["КПС"] = df.apply(calculate_kps, axis=1)
    df_filtered = df[df["Индекс изменений"] > 0].sort_values(by="Заработали", ascending=False)
    output_file_path = os.path.join(output_folder, f"sorted_{file_name}")
    df_filtered.to_excel(output_file_path, index=False)
    print(f"Результат сохранён в файл: {output_file_path}")


input_folder = "Datasets/pre_ans"
output_folder = "Datasets/pre_ans_sorted"
os.makedirs(output_folder, exist_ok=True)
file_list = [file_name for file_name in os.listdir(input_folder) if file_name.endswith((".xls", ".xlsx"))]

if __name__ == "__main__":
    with ProcessPoolExecutor() as executor:
        func = partial(process_file, input_folder=input_folder, output_folder=output_folder)
        executor.map(func, file_list)
