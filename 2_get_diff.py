import os
import pandas as pd
import multiprocessing

def process_competitor(competitor, apteka_items, competitors_dir, output_dir):
    competitor_path = os.path.join(competitors_dir, competitor)
    if not os.path.isdir(competitor_path):
        return

    diff_folder = os.path.join(output_dir, f"diff_{competitor}")
    os.makedirs(diff_folder, exist_ok=True)

    supported_extensions = ('.xls', '.xlsx', '.xlsm')

    for file in os.listdir(competitor_path):
        if file.endswith(supported_extensions):
            file_path = os.path.join(competitor_path, file)
            try:
                df = pd.read_excel(file_path, dtype=str).dropna()
                competitor_items = set(df.iloc[:, 0].tolist())

                diff_items = competitor_items - apteka_items
                if diff_items:
                    diff_df = df[df.iloc[:, 0].isin(diff_items)]
                    output_file_name = f"diff_{os.path.splitext(file)[0]}.xlsx"
                    output_file = os.path.join(diff_folder, output_file_name)
                    diff_df.to_excel(output_file, index=False)
                    print(f"Разница сохранена: {output_file}")
            except Exception as e:
                print(f"Ошибка обработки файла {file_path}: {e}")

def find_differences(apteka_file, competitors_dir, output_dir):
    try:
        if apteka_file.endswith(('.xls', '.xlsx', '.xlsm')):
            apteka_df = pd.read_excel(apteka_file, usecols=[0], dtype=str).dropna()
            apteka_items = set(apteka_df.iloc[:, 0].tolist())
        else:
            print(f"Неподдерживаемый формат файла аптеки: {apteka_file}")
            return
    except Exception as e:
        print(f"Ошибка при загрузке файла аптеки: {e}")
        return

    os.makedirs(output_dir, exist_ok=True)
    competitors = [comp for comp in os.listdir(competitors_dir) if os.path.isdir(os.path.join(competitors_dir, comp))]

    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        pool.starmap(process_competitor, [(comp, apteka_items, competitors_dir, output_dir) for comp in competitors])

apteka_file_path = input('My_apteka_path:')
competitors_directory = "Datasets/data/comp"
output_directory = "Datasets/diff_comp"

find_differences(apteka_file_path, competitors_directory, output_directory)
