import asyncio
import aiohttp
import random
import csv
import re
import os
from bs4 import BeautifulSoup
from datetime import datetime
import ssl

async def fetch_page(session, url, page):
    try:
        params = {'page': page}
        async with session.get(url, params=params) as response:
            if response.status != 200:
                print(f"Ошибка при загрузке страницы {page}: {response.status}")
                return None
            html_content = await response.text()
            return html_content
    except Exception as e:
        print(f"Исключение при загрузке страницы {page}: {e}")
        return None

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

        # Извлекаем данные из колонок
        name_div = cols[0].find('a')
        type_span = cols[0].find('span', class_='capture')
        price_div = cols[4].find(class_='price-value')
        quantity_div = cols[4].find(class_='capture')
        quantity_text = quantity_div.text.strip() if quantity_div else ''

        # Инициализируем переменные
        total_quantity = ''
        only_quantity = ''

        if 'от' in quantity_text.lower():
            # Обрабатываем случай с "от" и парсим tooltip
            tooltip_body = cols[4].find('div', class_='tooltip-info-body')
            if tooltip_body:
                total_quantity_value = 0.0
                # Находим все строки с информацией о количестве
                quantities = tooltip_body.find_all('div', class_='tooltip-info-table-tr')
                for q in quantities:
                    tds = q.find_all('div', class_='tooltip-info-table-td')
                    if len(tds) >= 2:
                        quantity_str = tds[1].text.strip()
                        # Извлекаем число с учетом дробных значений
                        match = re.search(r'([\d\.,]+)', quantity_str)
                        if match:
                            num_str = match.group(1).replace(',', '.')
                            try:
                                quantity = float(num_str)
                                total_quantity_value += quantity
                            except ValueError:
                                continue  # Пропускаем, если не число
                total_quantity = f"{total_quantity_value} упаковок"
                only_quantity = str(total_quantity_value)
            else:
                total_quantity = 'Не удалось получить количество'
                only_quantity = ''
        else:
            # Извлекаем количество как обычно
            total_quantity = quantity_text
            quantity_match = re.search(r'([\d\.,]+)', total_quantity)
            if quantity_match:
                num_str = quantity_match.group(1).replace(',', '.')
                only_quantity = num_str
            else:
                only_quantity = ''

        item = {
            'name': name_div.text.strip() if name_div else '',
            'item_type': type_span.text.strip() if type_span else '',
            'form': cols[1].text.strip(),
            'producer': cols[2].text.strip(),
            'price': price_div.text.strip() if price_div else '',
            'quantity': total_quantity,
            'only_quantity': only_quantity
        }
        data.append(item)
    return data

def clean_single_item(item):
    # Очищаем и структурируем данные
    form_parts = item['form'].split('\n')
    producer_parts = item['producer'].split('\n')

    cleaned_item = {
        'name': item['name'],
        'item_type': item['item_type'],
        'item_form': form_parts[0].strip(),
        'prescription': form_parts[-1].strip() if len(form_parts) > 1 else '',
        'manufacturer': producer_parts[0].strip(),
        'country': producer_parts[-1].strip() if len(producer_parts) > 1 else '',
        'price': item['price'],
        'quantity': item['quantity'],
        'only_quantity': item['only_quantity']
    }
    return cleaned_item

def save_to_csv(cleaned_data, file_name):
    file_exists = os.path.isfile(file_name)

    with open(file_name, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['name', 'item_type', 'item_form', 'prescription', 'manufacturer', 'country', 'price', 'quantity', 'only_quantity']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        for item in cleaned_data:
            writer.writerow(item)

async def get_total_positions(session, url):
    try:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"Ошибка при загрузке страницы: {response.status}")
                return None
            html_content = await response.text()
            soup = BeautifulSoup(html_content, 'html.parser')
            label_div = soup.find('div', class_='bttn-check')
            if label_div:
                label = label_div.find('label')
                if label:
                    text = label.get_text(strip=True)
                    match = re.search(r'Найдено позиций в продаже\s*-\s*(\d+)', text)
                    if match:
                        total_positions = int(match.group(1))
                        return total_positions
            print("Не удалось определить общее количество позиций.")
            return None
    except Exception as e:
        print(f"Исключение при получении общего количества позиций: {e}")
        return None

async def get_all_pages(url, file_name):
    # Создаем SSL-контекст
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        total_positions = await get_total_positions(session, url)
        if not total_positions:
            print("Не удалось получить общее количество позиций.")
            return

        items_per_page = 20  # Укажите фактическое количество позиций на странице
        total_pages = total_positions // items_per_page + (1 if total_positions % items_per_page else 0)
        page = 1
        while page <= total_pages:
            html_content = await fetch_page(session, url, page)
            if not html_content:
                break

            page_data = parse_table(html_content)
            if not page_data:
                print(f"Нет данных на странице {page}.")
                break

            cleaned_data = [clean_single_item(item) for item in page_data]
            save_to_csv(cleaned_data, file_name)

            # Выводим прогресс
            os.system('cls' if os.name == 'nt' else 'clear')
            print(f"Страница {page}/{total_pages} обработана и данные сохранены.")

            page += 1
            await asyncio.sleep(random.uniform(0.5, 1.5))  # Небольшая пауза между запросами

def get_next_file_index(path_to_save, base_filename):
    existing_files = os.listdir(path_to_save)
    indices = []
    pattern = re.compile(r'^(\d+)_{}.*\.csv$'.format(re.escape(base_filename)))
    for filename in existing_files:
        match = pattern.match(filename)
        if match:
            indices.append(int(match.group(1)))
    return max(indices) + 1 if indices else 0

def get_parser_data(url, path_to_save):
    # Определяем базовое имя файла без индекса
    base_filename = 'parsed_data'

    # Получаем следующий индекс
    index = get_next_file_index(path_to_save, base_filename)

    # Время для уникальности имени файла
    current_time = datetime.now().strftime('%Y-%m-%d_%H-%M')

    # Формируем полное имя файла с индексом и временем
    file_name = os.path.join(path_to_save, f'{index}_{base_filename}_{current_time}.csv')

    # Запускаем асинхронный парсер
    asyncio.run(get_all_pages(url, file_name))

    print(f'Данные успешно сохранены в файле: {file_name}')