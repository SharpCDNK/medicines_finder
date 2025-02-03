import os
import pandas as pd
import multiprocessing

def process_competitor(competitor, apteka_items, competitors_dir, output_dir):
    competitor_path = os.path.join(competitors_dir, competitor)
    if not os.path.isdir(competitor_path):
        return

    diff_folder = os.path.join(output_dir, f"diff_{competitor}")
    os.makedirs(diff_folder, exist_ok=True)

    supported_extensions = ('.csv', '.xls', '.xlsx', '.xlsm')

    for file in os.listdir(competitor_path):
        if file.endswith(supported_extensions):
            file_path = os.path.join(competitor_path, file)
            try:
                # Определяем тип файла и используем соответствующий метод чтения
                if file.endswith('.csv'):
                    # Читаем CSV-файл
                    df = pd.read_csv(file_path, dtype=str).dropna()
                    # Преобразуем и сохраняем в Excel
                    excel_file_path = os.path.splitext(file_path)[0] + '.xlsx'
                    df.to_excel(excel_file_path, index=False)
                    print(f"CSV-файл {file} преобразован в Excel: {excel_file_path}")
                    # Обновляем путь и имя файла для дальнейшей обработки
                    file_path = excel_file_path
                    file = os.path.basename(file_path)
                    # Теперь работаем с Excel-файлом
                    df = pd.read_excel(file_path, dtype=str).dropna()
                elif file.endswith(('.xls', '.xlsx', '.xlsm')):
                    df = pd.read_excel(file_path, dtype=str).dropna()
                else:
                    continue  # Если расширение не поддерживается, пропускаем файл

                competitor_items = set(df.iloc[:, 0].tolist())

                diff_items = competitor_items - apteka_items
                if diff_items:
                    diff_df = df[df.iloc[:, 0].isin(diff_items)]
                    # Формируем имя выходного файла с расширением .xlsx
                    output_file_name = f"diff_{os.path.splitext(file)[0]}.xlsx"
                    output_file = os.path.join(diff_folder, output_file_name)
                    diff_df.to_excel(output_file, index=False)
                    print(f"Разница сохранена: {output_file}")
            except Exception as e:
                print(f"Ошибка обработки файла {file_path}: {e}")

def find_differences(apteka_file, competitors_dir, output_dir):
    try:
        # Определяем тип файла аптеки и используем соответствующий метод чтения
        if apteka_file.endswith('.csv'):
            apteka_df = pd.read_csv(apteka_file, usecols=[0], dtype=str).dropna()
            # Преобразуем и сохраняем в Excel
            excel_file_path = os.path.splitext(apteka_file)[0] + '.xlsx'
            apteka_df.to_excel(excel_file_path, index=False)
            print(f"CSV-файл аптеки преобразован в Excel: {excel_file_path}")
            # Обновляем путь для дальнейшей работы
            apteka_file = excel_file_path
            apteka_df = pd.read_excel(apteka_file, usecols=[0], dtype=str).dropna()
        elif apteka_file.endswith(('.xls', '.xlsx', '.xlsm')):
            apteka_df = pd.read_excel(apteka_file, usecols=[0], dtype=str).dropna()
        else:
            print(f"Неподдерживаемый формат файла аптеки: {apteka_file}")
            return

        apteka_items = set(apteka_df.iloc[:, 0].tolist())
    except Exception as e:
        print(f"Ошибка при загрузке файла аптеки: {e}")
        return

    os.makedirs(output_dir, exist_ok=True)
    competitors = [comp for comp in os.listdir(competitors_dir) if os.path.isdir(os.path.join(competitors_dir, comp))]

    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        pool.starmap(process_competitor, [(comp, apteka_items, competitors_dir, output_dir) for comp in competitors])

if __name__ == "__main__":
    # Пример использования
    apteka_file_path = "Datasets/our_pharmacies/nasha_apteka_9/parsed_data_2025-02-03_21-41.csv"
    competitors_directory = "Datasets/competitors"
    output_directory = "Datasets/diff_comp"

    find_differences(apteka_file_path, competitors_directory, output_directory)
