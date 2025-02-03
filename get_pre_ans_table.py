import os
import pandas as pd
import re

def generate_pre_ans_table(analysis_dir, diff_comp_dir, output_dir):
    if not os.path.exists(analysis_dir):
        print(f"Папка {analysis_dir} не существует.")
        return
    if not os.path.exists(diff_comp_dir):
        print(f"Папка {diff_comp_dir} не существует.")
        return
    os.makedirs(output_dir, exist_ok=True)

    for competitor in os.listdir(analysis_dir):
        competitor_analysis_path = os.path.join(analysis_dir, competitor, f"{competitor}_list_for_analis.xlsx")
        competitor_diff_path = os.path.join(diff_comp_dir, f"{competitor}")

        if not os.path.exists(competitor_analysis_path) or not os.path.isdir(competitor_diff_path):
            print(f"Пропускаю {competitor}: отсутствуют необходимые файлы или папки.")
            continue

        try:
            product_df = pd.read_excel(competitor_analysis_path, dtype=str).dropna()
        except Exception as e:
            print(f"Ошибка при чтении файла {competitor_analysis_path}: {e}")
            continue

        key_columns = ['name', 'item_type', 'item_form', 'prescription', 'manufacturer', 'country']
        product_df['key'] = product_df[key_columns].agg('|'.join, axis=1)
        pre_ans = pd.DataFrame({'key': product_df['key']})
        pre_ans[key_columns] = product_df[key_columns]

        print(f"\nОбрабатываю товары для конкурента: {competitor}")
        print(f"Читаю список товаров из: {competitor_analysis_path}")

        diff_files = []
        for diff_file in os.listdir(competitor_diff_path):
            if not diff_file.endswith(".xls") and not diff_file.endswith(".xlsx"):
                continue
            match = re.search(r"diff_parsed_data_(\d+)_", diff_file)
            if match:
                index = int(match.group(1))
                diff_files.append((index, diff_file))

        diff_files = sorted(diff_files, key=lambda x: x[0])

        for index, diff_file in diff_files:
            diff_file_path = os.path.join(competitor_diff_path, diff_file)
            match = re.search(r"diff_parsed_data_\d+_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2})", diff_file)
            if not match:
                print(f"Файл {diff_file} не соответствует шаблону имени. Пропускаю.")
                continue
            date_time = match.group(1).replace("_", " ").replace("-", ":", 1)

            print(f"Читаю diff файл: {diff_file_path} (дата: {date_time})")

            try:
                df = pd.read_excel(diff_file_path, dtype=str).dropna()
            except Exception as e:
                print(f"Ошибка при чтении файла {diff_file_path}: {e}")
                continue

            if df.shape[1] < 9:
                print(f"Файл {diff_file_path} имеет менее 9 колонок. Пропускаю.")
                continue

            df['key'] = df.iloc[:, :6].agg('|'.join, axis=1)
            price_info = df.set_index('key').iloc[:, 6].to_dict()
            quantity_info = df.set_index('key').iloc[:, 8].to_dict()

            pre_ans[f"Цена {date_time}"] = pre_ans['key'].map(price_info).fillna("-")
            pre_ans[f"Количество {date_time}"] = pre_ans['key'].map(quantity_info).fillna("-")

        pre_ans.drop(columns=['key'], inplace=True)
        output_file = os.path.join(output_dir, f"pre_ans_{competitor}.xlsx")
        pre_ans.to_excel(output_file, index=False)
        print(f"pre_ans сохранен: {output_file}")

# Пример использования
analysis_directory = "Datasets/list_for_analis"
comparison_directory = "Datasets/diff_comp"
output_directory = "Datasets/pre_ans"

generate_pre_ans_table(analysis_directory, comparison_directory, output_directory)
