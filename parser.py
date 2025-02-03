import time
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
from datetime import datetime
from utils import get_latest_file_path  # Ensure this method is defined


def parse_product_card(card):
    name = card.find('div', class_='product-card__name').text.strip()
    form = card.find('div', class_='product-card__fas').text.strip()
    producer = card.find('div', class_='product-card__country').text.strip()

    price_info = card.find('span', class_='second-price').text.strip()
    quantity_info = card.find('div', class_='pharmacy-card__count').text.strip()

    return {
        'name': name,
        'form': form,
        'producer': producer,
        'price': price_info,
        'quantity': quantity_info
    }


def parse_table(soup):
    product_cards = soup.find_all('div', class_='product-card')
    if not product_cards:
        print("Продукты не найдены на странице.")
        return []

    data = []
    for card in product_cards:
        item = parse_product_card(card)
        data.append(item)

    return data


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

        # Check for "показать еще" button
        next_button = soup.find('a', class_='modal-link get-next-page')
        if next_button:
            print("Кнопка 'показать еще' найдена. Переход к следующей странице.")
            # Simulate clicking the button by incrementing the page number
            page += 1
        else:
            print("Кнопка 'показать еще' не найдена. Завершение.")
            break

        # Pause between requests
        time.sleep(random.uniform(2, 4))

    cleaned_data = clean_data(all_data)

    # Create DataFrame and write to Excel
    df = pd.DataFrame(cleaned_data)

    last_file = get_latest_file_path(path_to_save)
    if last_file is None:
        last_index = 0
    else:
        match = re.search(r'data_(\d+)_2025', last_file)

        if match:
            last_index = int(match.group(1))

    # Create new index
    new_index = last_index + 1
    current_time = datetime.now().strftime('%Y-%m-%d_%H-%M')

    # Create filename with date and time
    file_name = f'parsed_data_{new_index}_{current_time}.xlsx'
    df.to_excel(os.path.join(path_to_save, file_name), index=False)

    print(f'Сохранен файл: {os.path.join(path_to_save, file_name)}')

# Example call
# get_parser_data('URL_СТРАНИЦЫ', 'ПУТЬ_К_ПАПКЕ_ДЛЯ_СОХРАНЕНИЯ')