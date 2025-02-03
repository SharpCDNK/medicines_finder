import os
import pandas as pd

# Пути к папкам и файлам
source_folder = 'Datasets/ans_sorted_comp'
destination_folder = 'Datasets/Finish'
destination_file = os.path.join(destination_folder, 'final_result.xlsx')

# Создаём пустой DataFrame для итоговых данных
final_df = pd.DataFrame()

# Обрабатываем каждый файл
for filename in os.listdir(source_folder):
    if filename.endswith('.xlsx') or filename.endswith('.xls'):
        file_path = os.path.join(source_folder, filename)

        # Считываем данные из файла
        data = pd.read_excel(file_path)

        # Выводим названия колонок для проверки
        print(f"Обработка файла: {filename}")
        print("Названия колонок:", data.columns.tolist())

        # Приводим названия колонок к нижнему регистру и убираем пробелы по краям
        data.columns = data.columns.str.strip().str.lower()

        # Словарь для переименования колонок
        rename_columns = {
            'цена min': 'Цена Мин',
            'цена мин': 'Цена Мин',
            'цена max': 'Цена Макс',
            'цена макс': 'Цена Макс',
            'медианная цена': 'Цена Медиум',
            'медианнаяцена': 'Цена Медиум',
            'индекс изменений': 'Индекс Изменений',
            'индексизменений': 'Индекс Изменений',
            'заработали': 'Заработали'
            # Добавьте другие соответствия, если требуется
        }

        # Переименовываем колонки согласно словарю
        data.rename(columns=rename_columns, inplace=True)

        # Проверяем наличие необходимых колонок
        required_columns = ['Цена Мин', 'Цена Макс', 'Цена Медиум', 'Индекс Изменений', 'Заработали']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            print(f"В файле {filename} отсутствуют колонки: {', '.join(missing_columns)}. Пропускаем этот файл.")
            continue

        # Получаем идентификатор файла
        file_identifier = '_'.join(filename.replace('.xlsx', '').split('_')[-2:])

        # Итерируемся по строкам текущего файла
        for index, row in data.iterrows():
            # Ключ для идентификации строки (первые 6 колонок)
            key = tuple(row.iloc[:6])

            if not final_df.empty:
                # Создаём маску для поиска строки с таким же ключом
                mask = final_df.apply(lambda x: tuple(x.iloc[:6]), axis=1) == key

                if mask.any():
                    # Если строка найдена, получаем её индекс
                    existing_index = final_df[mask].index[0]

                    # Обновляем "Цена Мин", если новая меньше
                    final_df.at[existing_index, 'Цена Мин'] = min(final_df.at[existing_index, 'Цена Мин'], row['Цена Мин'])

                    # Обновляем "Цена Макс", если новая больше
                    final_df.at[existing_index, 'Цена Макс'] = max(final_df.at[existing_index, 'Цена Макс'], row['Цена Макс'])

                    # Увеличиваем "Индекс Изменений"
                    final_df.at[existing_index, 'Индекс Изменений'] += row['Индекс Изменений']

                    # Увеличиваем "Заработали"
                    final_df.at[existing_index, 'Заработали'] += row['Заработали']

                    # Добавляем информацию об источнике и изменениях
                    source_info = (
                        f"{file_identifier}, цена мин = {row['Цена Мин']}, цена макс = {row['Цена Макс']}, "
                        f"цена медиум = {row['Цена Медиум']}, индекс изменений = {row['Индекс Изменений']}, "
                        f"заработали = {row['Заработали']}"
                    )
                    final_df.at[existing_index, 'Источники'] += f"; {source_info}"

                    # Увеличиваем счётчик "Аптеки"
                    final_df.at[existing_index, 'Аптеки'] += 1

                else:
                    # Если такой строки нет, добавляем её как новую
                    new_row = row.iloc[:11].copy()
                    new_row['Источники'] = (
                        f"{file_identifier}, цена мин = {row['Цена Мин']}, цена макс = {row['Цена Макс']}, "
                        f"цена медиум = {row['Цена Медиум']}, индекс изменений = {row['Индекс Изменений']}, "
                        f"заработали = {row['Заработали']}"
                    )
                    new_row['Аптеки'] = 1
                    # Преобразуем строку в DataFrame и объединяем
                    final_df = pd.concat([final_df, pd.DataFrame([new_row])], ignore_index=True)
            else:
                # Если final_df пустой, создаём его из первой строки
                new_row = row.iloc[:11].copy()
                new_row['Источники'] = (
                    f"{file_identifier}, цена мин = {row['Цена Мин']}, цена макс = {row['Цена Макс']}, "
                    f"цена медиум = {row['Цена Медиум']}, индекс изменений = {row['Индекс Изменений']}, "
                    f"заработали = {row['Заработали']}"
                )
                new_row['Аптеки'] = 1
                final_df = pd.DataFrame([new_row])

# Убеждаемся, что папка назначения существует
if not os.path.exists(destination_folder):
    os.makedirs(destination_folder)

# Сохраняем итоговый DataFrame в Excel-файл
final_df.to_excel(destination_file, index=False)
