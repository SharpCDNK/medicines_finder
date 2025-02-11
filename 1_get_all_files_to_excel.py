import os
import pandas as pd
import shutil
from multiprocessing import Pool, cpu_count

# Исходные папки
competitor_folder = "Datasets/competitors/"
our_pharmacies_folder = "Datasets/our_pharmacies/"

# Целевые папки
target_comp_folder = "Datasets/data/comp/"
target_our_pharmacies_folder = "Datasets/data/our_pharmacies/"

# Убедимся, что целевые папки существуют
os.makedirs(target_comp_folder, exist_ok=True)
os.makedirs(target_our_pharmacies_folder, exist_ok=True)


def process_file(args):
    source_path, source_folder, target_folder = args

    # Определяем относительный путь для сохранения структуры
    relative_path = os.path.relpath(os.path.dirname(source_path), source_folder)
    target_path_dir = os.path.join(target_folder, relative_path)
    os.makedirs(target_path_dir, exist_ok=True)

    file = os.path.basename(source_path)

    if file.lower().endswith(".csv"):
        # Преобразуем .csv в .xlsx
        try:
            df = pd.read_csv(source_path)
            new_file_name = os.path.splitext(file)[0] + ".xlsx"
            target_path = os.path.join(target_path_dir, new_file_name)
            df.to_excel(target_path, index=False)
            print(f"CSV файл преобразован и сохранён как Excel: {target_path}")
        except Exception as e:
            print(f"Ошибка при преобразовании файла {source_path}: {e}")
    else:
        # Перемещаем остальные файлы без изменений
        try:
            target_path = os.path.join(target_path_dir, file)
            shutil.move(source_path, target_path)
            print(f"Файл перемещён: {target_path}")
        except Exception as e:
            print(f"Ошибка при перемещении файла {source_path}: {e}")


def process_and_move_files(source_folder, target_folder):

    """
    Перемещает все файлы из source_folder в target_folder,
    сохраняя структуру папок. Если файл имеет расширение .csv,
    преобразует его в .xlsx при перемещении.
    """

    file_paths = []
    for root, _, files in os.walk(source_folder):
        for file in files:
            source_path = os.path.join(root, file)
            file_paths.append((source_path, source_folder, target_folder))

    # Используем пул процессов для параллельной обработки файлов
    with Pool(cpu_count()) as pool:
        pool.map(process_file, file_paths)


# Обрабатываем папки
process_and_move_files(competitor_folder, target_comp_folder)
process_and_move_files(our_pharmacies_folder, target_our_pharmacies_folder)

print("Все файлы успешно обработаны и перенесены.")
