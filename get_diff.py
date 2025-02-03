import os
import pandas as pd


def find_differences(apteka_file, competitors_dir, output_dir):
    # Загружаем файл аптеки
    apteka_df = pd.read_excel(apteka_file, usecols=[0], dtype=str).dropna()
    apteka_items = set(apteka_df.iloc[:, 0].tolist())

    # Проверяем, существует ли выходная папка
    os.makedirs(output_dir, exist_ok=True)

    # Обход папок конкурентов
    for competitor in os.listdir(competitors_dir):
        competitor_path = os.path.join(competitors_dir, competitor)
        if os.path.isdir(competitor_path):
            diff_folder = os.path.join(output_dir, f"diff_{competitor}")
            os.makedirs(diff_folder, exist_ok=True)

            # Читаем все файлы конкурента и сравниваем их по отдельности
            for file in os.listdir(competitor_path):
                if file.endswith(".xls") or file.endswith(".xlsx"):
                    file_path = os.path.join(competitor_path, file)
                    df = pd.read_excel(file_path, dtype=str).dropna()
                    competitor_items = set(df.iloc[:, 0].tolist())

                    # Находим разницу
                    diff_items = competitor_items - apteka_items

                    # Записываем результат, если есть отличия
                    if diff_items:
                        diff_df = df[df.iloc[:, 0].isin(diff_items)]
                        output_file = os.path.join(diff_folder, f"diff_{file}")
                        diff_df.to_excel(output_file, index=False)
                        print(f"Разница сохранена: {output_file}")


# Пример использования
apteka_file_path = "Datasets/our_pharmacies/apteka_9/parsed_data_1_2025-02-02_18-19.xlsx"  # Укажите путь к файлу аптеки
competitors_directory = "Datasets/competitors"  # Папка с конкурентами
output_directory = "Datasets/diff_comp"  # Папка для разницы

find_differences(apteka_file_path, competitors_directory, output_directory)
