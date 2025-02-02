import os
import pandas as pd
import re


def generate_pre_ans_table(analysis_dir, diff_comp_dir, output_dir):
    # Проверка существования основных директорий
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

        # Загружаем список товаров для анализа
        try:
            product_df = pd.read_excel(competitor_analysis_path, dtype=str).dropna()
        except Exception as e:
            print(f"Ошибка при чтении файла {competitor_analysis_path}: {e}")
            continue

        product_names = product_df.iloc[:, 0].tolist()
        pre_ans = pd.DataFrame({"Наименование": product_names})

        print(f"\nОбрабатываю товары для конкурента: {competitor}")
        print(f"Читаю список товаров из: {competitor_analysis_path}")

        # Собираем все diff файлы с их индексами
        diff_files = []
        for diff_file in os.listdir(competitor_diff_path):
            if not diff_file.endswith(".xls") and not diff_file.endswith(".xlsx"):
                continue

            # Ищем индекс в имени файла
            match = re.search(r"difference_parsed_data_(\d+)_", diff_file)
            if match:
                index = int(match.group(1))  # Извлекаем индекс
                diff_files.append((index, diff_file))

        # Сортируем файлы по индексам
        diff_files = sorted(diff_files, key=lambda x: x[0])

        # Обход файлов в порядке возрастания индексов
        for index, diff_file in diff_files:
            diff_file_path = os.path.join(competitor_diff_path, diff_file)
            match = re.search(r"difference_parsed_data_\d+_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2})", diff_file)

            if not match:
                print(f"Файл {diff_file} не соответствует шаблону имени. Пропускаю.")
                continue

            date_time = match.group(1).replace("_", " ").replace("-", ":", 1)

            print(f"Читаю diff файл для конкурента {competitor}: {diff_file_path} (дата: {date_time})")

            try:
                df = pd.read_excel(diff_file_path, dtype=str).dropna()
            except Exception as e:
                print(f"Ошибка при чтении файла {diff_file_path}: {e}")
                continue

            if df.shape[1] < 9:
                print(f"Файл {diff_file_path} имеет менее 9 колонок. Пропускаю.")
                continue

            # Берем только 1 (наименование), G (цена) и I (количество)
            df = df.iloc[:, [0, 6, 8]]
            price_info = {row[0]: row[1] for row in df.itertuples(index=False, name=None)}
            quantity_info = {row[0]: row[2] for row in df.itertuples(index=False, name=None)}

            pre_ans[f"Цена {date_time}"] = pre_ans["Наименование"].map(price_info).fillna("-")
            pre_ans[f"Количество {date_time}"] = pre_ans["Наименование"].map(quantity_info).fillna("-")

        # Сохраняем pre_ans в output_dir для каждого конкурента
        output_file = os.path.join(output_dir, f"pre_ans_{competitor}.xlsx")
        pre_ans.to_excel(output_file, index=False)
        print(f"pre_ans сохранен: {output_file}")


# Пример использования
analysis_directory = "Datasets/list_for_analis"
comparison_directory = "Datasets/diff_comp"
output_directory = "Datasets/pre_ans"

generate_pre_ans_table(analysis_directory, comparison_directory, output_directory)