import pandas as pd
import os

# Путь к папке с файлами
folder_path = 'Datasets/pre_ans_sorted'

# Чтение всех xls и xlsx файлов в папке
all_files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith(('.xls', '.xlsx'))]

# Проверка наличия файлов
if not all_files:
    raise ValueError("No .xls or .xlsx files found in the specified folder")

# Функция для очистки и преобразования цен в числовой формат
def clean_price(price):
    try:
        if isinstance(price, str):
            if 'от' in price:
                price = price.replace('от', '').strip()
            return float(price.replace('р.', '').replace(',', '.').strip())
        return price
    except ValueError:
        return None

# Объединение всех файлов в один DataFrame
df_list = [pd.read_excel(file) for file in all_files]

if not df_list:
    raise ValueError("No data to concatenate. Please check the files.")

combined_df = pd.concat(df_list, ignore_index=True)

# Преобразование цен в числовой формат
combined_df['Цена min'] = combined_df['Цена min'].apply(clean_price)
combined_df['Цена max'] = combined_df['Цена max'].apply(clean_price)

# Удаление строк с нечисловыми значениями в ценах
combined_df = combined_df.dropna(subset=['Цена min', 'Цена max'])

# Группировка по первым 6 столбцам и агрегирование данных
grouped_df = combined_df.groupby(['name', 'item_type', 'item_form', 'prescription', 'manufacturer', 'country']).agg({
    'Цена min': 'mean',
    'Цена max': 'mean',
    'Индекс изменений': 'sum',
    'Заработали': 'sum'
}).reset_index()

# Добавление столбца медианной цены
grouped_df['Медианная цена'] = (grouped_df['Цена min'] + grouped_df['Цена max']) / 2

# Добавление столбца о наличии в разных аптеках
grouped_df['Разные аптеки'] = combined_df.groupby(['name', 'item_type', 'item_form', 'prescription', 'manufacturer', 'country']).size().reset_index(name='count')['count']

# Сохранение результата в новый файл
grouped_df.to_excel('Объединенный_и_Отсортированный.xlsx', index=False)

print("Скрипт выполнен успешно и файл сохранен!")
