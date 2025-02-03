import asyncio
import aiohttp
import random
import pandas as pd
import re
import os
from bs4 import BeautifulSoup
from datetime import datetime
from multiprocessing import Pool
from utils import get_latest_file_path  # Убедись, что этот метод определён


def parse_table(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
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


def clean_single_item(item):
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
    return cleaned_item


def clean_data(data):
    with Pool() as pool:
        cleaned_data = pool.map(clean_single_item, data)
    return cleaned_data


async def fetch_page(session, url, page):
    try:
        async with session.get(f"{url}?page={page}") as response:
            if response.status != 200:
                print(f"Ошибка при загрузке страницы {page}: {response.status}")
                return None
            html_content = await response.text()
            return html_content
    except Exception as e:
        print(f"Исключение при загрузке страницы {page}: {e}")
        return None


async def get_all_pages(url):
    all_data = []
    page = 1

    async with aiohttp.ClientSession() as session:
        while True:
            html_content = await fetch_page(session, url, page)
            if not html_content:
                break

            page_data = parse_table(html_content)
            if not page_data:
                print(f"Нет данных на странице {page}. Завершение.")
                break

            all_data.extend(page_data)
            print(f"Страница {page} обработана.")

            page += 1
            await asyncio.sleep(random.uniform(0.5, 1.5))  # Небольшая пауза между запросами

    return all_data


def save_to_excel(cleaned_data, path_to_save):
    df = pd.DataFrame(cleaned_data)

    last_file = get_latest_file_path(path_to_save)
    if last_file is None:
        last_index = 0
    else:
        match = re.search(r'parsed_data_(\d+)_', last_file)
        if match:
            last_index = int(match.group(1))
        else:
            last_index = 0

    new_index = last_index + 1
    current_time = datetime.now().strftime('%Y-%m-%d_%H-%M')

    file_name = f'parsed_data_{new_index}_{current_time}.xlsx'
    df.to_excel(os.path.join(path_to_save, file_name), index=False)

    print(f'Сохранён файл: {os.path.join(path_to_save, file_name)}')


def get_parser_data(url, path_to_save):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    all_data = loop.run_until_complete(get_all_pages(url))

    if all_data:
        cleaned_data = clean_data(all_data)
        save_to_excel(cleaned_data, path_to_save)
    else:
        print("Нет данных для обработки.")
