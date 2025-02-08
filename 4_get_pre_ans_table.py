import os
import pandas as pd
import re
import multiprocessing
from collections import defaultdict
import numpy as np
from openpyxl.styles import PatternFill

gray_fill = PatternFill(start_color="C0C0C0", end_color="C0C0C0", fill_type="solid")


def process_competitor(competitor, analysis_dir, diff_comp_dir, output_dir):
    competitor_analysis_path = os.path.join(analysis_dir, competitor, f"{competitor}_list_for_analis.xlsx")
    competitor_diff_path = os.path.join(diff_comp_dir, competitor)

    if not os.path.exists(competitor_analysis_path) or not os.path.isdir(competitor_diff_path):
        print(f"Пропускаю {competitor}: отсутствуют необходимые файлы или папки.")
        return

    try:
        product_df = pd.read_excel(competitor_analysis_path, dtype=str).fillna('')
    except Exception as e:
        print(f"Ошибка при чтении файла {competitor_analysis_path}: {e}")
        return

    key_columns = ['name', 'item_type', 'item_form', 'prescription', 'manufacturer', 'country']
    product_df['key'] = product_df[key_columns].agg('|'.join, axis=1)
    pre_ans = product_df[['key'] + key_columns].copy()
    pre_ans['Цена min'] = 0
    pre_ans['Цена max'] = 0
    pre_ans['Медианная цена'] = 0

    prices_dict = defaultdict(list)

    diff_files = []
    for diff_file in os.listdir(competitor_diff_path):
        if not diff_file.endswith(('.xls', '.xlsx')):
            continue
        match = re.search(r"diff_(\d+)_parsed_data_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2})", diff_file)
        if match:
            index = int(match.group(1))
            date_time = match.group(2)
            diff_files.append((index, date_time, diff_file))

    diff_files.sort(key=lambda x: x[0])

    for index, date_time, diff_file in diff_files:
        diff_file_path = os.path.join(competitor_diff_path, diff_file)

        try:
            df = pd.read_excel(diff_file_path, dtype=str).fillna('')
        except Exception as e:
            print(f"Ошибка при чтении файла {diff_file_path}: {e}")
            continue

        if 'key' not in df.columns:
            df['key'] = df.iloc[:, :6].agg('|'.join, axis=1)

        price_column_name = 'price'
        quantity_column_name = 'only_quantity'

        if price_column_name not in df.columns or quantity_column_name not in df.columns:
            print(f"В файле {diff_file} отсутствуют колонки '{price_column_name}' или '{quantity_column_name}'.")
            continue

        quantity_info = df.set_index('key')[quantity_column_name].to_dict()

        for idx, row in df.iterrows():
            key = row['key']
            price_str = str(row[price_column_name])

            match_price = re.search(r"([\d\s]+[.,]?\d*)", price_str)
            if match_price:
                price_value = match_price.group(1)
                price_value = price_value.replace(',', '.').replace(' ', '')
                try:
                    price = float(price_value)
                    prices_dict[key].append(price)
                except (ValueError, TypeError):
                    continue

        pre_ans[f"Количество {index}_{date_time}"] = pre_ans['key'].map(quantity_info).fillna(0)

    if not prices_dict:
        print("Внимание: словарь цен пуст. Проверьте корректность обработки цен.")

    def compute_price_statistics(prices):
        if prices:
            return min(prices), max(prices), np.median(prices)
        return 0, 0, 0

    stats = pre_ans['key'].apply(lambda k: compute_price_statistics(prices_dict.get(k, [])))
    pre_ans['Цена min'] = stats.apply(lambda x: x[0])
    pre_ans['Цена max'] = stats.apply(lambda x: x[1])
    pre_ans['Медианная цена'] = stats.apply(lambda x: x[2])

    pre_ans.drop(columns=['key'], inplace=True)
    output_file = os.path.join(output_dir, f"pre_ans_{competitor}.xlsx")
    pre_ans.to_excel(output_file, index=False)
    print(f"\npre_ans сохранен: {output_file}")

def generate_pre_ans_table(analysis_dir, diff_comp_dir, output_dir):
    if not os.path.exists(analysis_dir):
        print(f"Папка {analysis_dir} не существует.")
        return
    if not os.path.exists(diff_comp_dir):
        print(f"Папка {diff_comp_dir} не существует.")
        return
    os.makedirs(output_dir, exist_ok=True)

    competitors = [comp for comp in os.listdir(analysis_dir) if os.path.isdir(os.path.join(analysis_dir, comp))]

    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        pool.starmap(process_competitor, [(comp, analysis_dir, diff_comp_dir, output_dir) for comp in competitors])

# Пример использования
analysis_directory = "Datasets/list_for_analis"
comparison_directory = "Datasets/diff_comp"
output_directory = "Datasets/pre_ans"

generate_pre_ans_table(analysis_directory, comparison_directory, output_directory)
