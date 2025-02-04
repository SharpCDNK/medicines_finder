import os
import pandas as pd
import multiprocessing

def process_competitor(competitor, apteka_items, competitors_dir, output_dir, total_files, processed_count):
    competitor_path = os.path.join(competitors_dir, competitor)
    if not os.path.isdir(competitor_path):
        return

    diff_folder = os.path.join(output_dir, f"diff_{competitor}")
    os.makedirs(diff_folder, exist_ok=True)

    supported_extensions = ('.xls', '.xlsx', '.xlsm')

    # Sorting files by index extracted from filenames
    files = sorted(
        [f for f in os.listdir(competitor_path) if f.endswith(supported_extensions)],
        key=lambda x: int(x.split('_')[0])
    )

    for file in files:
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
        finally:
            # Directly incrementing the processed_count
            processed_count.value += 1
            print(f"Загружено {processed_count.value} из {total_files} файлов")

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
    competitors = [
        comp for comp in os.listdir(competitors_dir)
        if os.path.isdir(os.path.join(competitors_dir, comp))
    ]

    # Calculating the total number of files to process
    total_files = sum(
        len([
            f for f in os.listdir(os.path.join(competitors_dir, comp))
            if f.endswith(('.xls', '.xlsx', '.xlsm'))
        ]) for comp in competitors
    )

    with multiprocessing.Manager() as manager:
        processed_count = manager.Value('i', 0)
        pool_args = [
            (comp, apteka_items, competitors_dir, output_dir, total_files, processed_count)
            for comp in competitors
        ]
        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            pool.starmap(process_competitor, pool_args)

apteka_file_path = input('My_apteka_path:')
competitors_directory = "Datasets/data/comp"
output_directory = "Datasets/diff_comp"

find_differences(apteka_file_path, competitors_directory, output_directory)
