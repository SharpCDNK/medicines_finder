import time
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
from datetime import datetime
from utils import get_latest_file_path  # Убедитесь, что этот метод определен

def parse_table(soup):
    table = soup.find('table', {'class': 'table-border'})
    if not table:
        print("Таблица не найдена на странице.")
        return []

    data = []
    for row in table.find_all('tr')[1:]:
        cols = row.find_all('td')
        if len(cols) < 5:
            continue
        item = {
            'name': cols[0].text.strip(),
            'form': cols[1].text.strip(),
            'producer': cols[2].text.strip(),
            'price': cols[4].find(class_='price-value').text.strip(),
            'quantity': cols[4].find(class_='capture').text.strip()
        }
        data.append(item)

    return data

def clean_data(data):
    cleaned_data = []
    for item in data:
        name_parts = item['name'].split('\n')
        form_parts = item['form'].split('\n')
        producer_parts = item['producer'].split('\n')

        quantity_match = re.search(r'\d+\.?\d*', item['quantity'])
        only_quantity = quantity_match.group(0) if quantity_match else ''

        cleaned_item = {
            'name': name_parts[0],
            'item_type': name_parts[-1].strip() if len(name_parts) > 1 else '',
            'item_form': form_parts[0],
            'prescription': form_parts[-1].strip() if len(form_parts) > 1 else '',
            'manufacturer': producer_parts[0].strip(),
            'country': producer_parts[-1].strip() if len(producer_parts) > 1 else '',
            'price': item['price'],
            'quantity': item['quantity'],
            'only_quantity': only_quantity
        }
        cleaned_data.append(cleaned_item)

    return cleaned_data

def get_parser_data(url, path_to_save):
    all_data = []
    page = 1

    while True:
        response = requests.get(f"{url}?page={page}")
        if response.status_code != 200:
            print(f"Ошибка при загрузке страницы {page}: {response.status_code}")
            break

        soup = BeautifulSoup(response.content, 'html.parser')
        page_data = parse_table(soup)

        if not page_data:
            print(f"Нет данных на странице {page}. Завершение.")
            break

        all_data.extend(page_data)
        print(f"Данные страницы {page}:")
        for item in page_data:
            print(item)

        # Пауза между запросами
        time.sleep(random.uniform(2, 4))
        page += 1

    cleaned_data = clean_data(all_data)

    # Создание DataFrame и запись в Excel
    df = pd.DataFrame(cleaned_data)

    last_file = get_latest_file_path(path_to_save)
    if last_file == None:
        last_index = 0
    else:
        match = re.search(r'data_(\d+)_2025', last_file)

        if match:
            last_index = int(match.group(1))


    # Создаем новый индекс
    new_index = last_index + 1
    current_time = datetime.now().strftime('%Y-%m-%d_%H-%M')

    # Создание имени файла с датой и временем
    file_name = f'parsed_data_{new_index}_{current_time}.xlsx'
    df.to_excel(os.path.join(path_to_save, file_name), index=False)

    print(f'Сохранен файл: {os.path.join(path_to_save, file_name)}')

# Пример вызова функции
# get_parser_data('URL_СТРАНИЦЫ', 'ПУТЬ_К_ПАПКЕ_ДЛЯ_СОХРАНЕНИЯ')