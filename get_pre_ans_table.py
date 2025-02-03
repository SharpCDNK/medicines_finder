import os
import pandas as pd
import re
import multiprocessing


def process_competitor(competitor, analysis_dir, diff_comp_dir, output_dir):
    competitor_analysis_path = os.path.join(analysis_dir, competitor, f"{competitor}_list_for_analis.xlsx")
    competitor_diff_path = os.path.join(diff_comp_dir, f"{competitor}")

    if not os.path.exists(competitor_analysis_path) or not os.path.isdir(competitor_diff_path):
        print(f"Пропускаю {competitor}: отсутствуют необходимые файлы или папки.")
        return

    try:
        product_df = pd.read_excel(competitor_analysis_path, dtype=str).dropna()
    except Exception as e:
        print(f"Ошибка при чтении файла {competitor_analysis_path}: {e}")
        return

    key_columns = ['name', 'item_type', 'item_form', 'prescription', 'manufacturer', 'country']
    product_df['key'] = product_df[key_columns].agg('|'.join, axis=1)
    pre_ans = product_df[['key'] + key_columns].copy()
    pre_ans['Цена min'] = "-"
    pre_ans['Цена max'] = "-"

    print(f"\nОбрабатываю товары для конкурента: {competitor}")
    diff_files = []
    for diff_file in os.listdir(competitor_diff_path):
        if not diff_file.endswith(('.xls', '.xlsx')):
            continue
        match = re.search(r"diff_parsed_data_(\d+)_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2})", diff_file)
        if match:
            index = int(match.group(1))
            date_time = match.group(2)
            diff_files.append((index, date_time, diff_file))

    diff_files.sort(key=lambda x: x[0])

    for index, date_time, diff_file in diff_files:
        diff_file_path = os.path.join(competitor_diff_path, diff_file)
        print(f"Читаю diff файл: {diff_file_path}")

        try:
            df = pd.read_excel(diff_file_path, dtype=str).dropna()
        except Exception as e:
            print(f"Ошибка при чтении файла {diff_file_path}: {e}")
            continue

        if df.shape[1] < 9:
            print(f"Файл {diff_file_path} имеет менее 9 колонок. Пропускаю.")
            continue

        df['key'] = df.iloc[:, :6].agg('|'.join, axis=1)
        price_info = df.set_index('key').iloc[:, 6].astype(str).to_dict()
        quantity_info = df.set_index('key').iloc[:, 8].to_dict()

        pre_ans['Цена min'] = pre_ans['key'].map(price_info).fillna(pre_ans['Цена min'])
        pre_ans['Цена max'] = pre_ans['key'].map(price_info).fillna(pre_ans['Цена max'])
        pre_ans[f"Количество {index}_{date_time}"] = pre_ans['key'].map(quantity_info).fillna("-")

    pre_ans.drop(columns=['key'], inplace=True)
    output_file = os.path.join(output_dir, f"pre_ans_{competitor}.xlsx")
    pre_ans.to_excel(output_file, index=False)
    print(f"pre_ans сохранен: {output_file}")


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
