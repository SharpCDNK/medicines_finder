import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
from datetime import datetime
from utils import get_latest_file_path  # Убедитесь, что этот метод определен

def parse_table(soup):
    # Найдите таблицу
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
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(service=Service(), options=options)
    try:
        driver.get(url)
        print("Страница загружена успешно.")
    except Exception as e:
        print(f"Ошибка при загрузке страницы: {e}")
        driver.quit()
        return

    try:
        carrets = WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.select-check-carret'))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", carrets[1])
        driver.execute_script("arguments[0].click();", carrets[1])

        option_100 = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//input[@value="100"]/following-sibling::label'))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", option_100)
        driver.execute_script("arguments[0].click();", option_100)

    except Exception as e:
        print("Ошибка при взаимодействии с элементами страницы:", e)
        driver.quit()
        return

    all_data = []

    while True:
        try:
            time.sleep(random.uniform(2, 4))  # Увеличиваем время ожидания
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            page_data = parse_table(soup)

            if not page_data:
                print("Нет данных для отображения на этой странице.")
                break

            all_data.extend(page_data)
            print("Данные текущей страницы:")
            for item in page_data:
                print(item)

            button_next = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '.table-pagination-next a'))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", button_next)
            driver.execute_script("arguments[0].click();", button_next)

        except Exception as e:
            print("Ошибка при переходе на следующую страницу:", e)
            break

    driver.quit()
    cleaned_data = clean_data(all_data)

    # Создание DataFrame и запись в Excel
    df = pd.DataFrame(cleaned_data)

    # Получаем путь к последнему файлу
    last_file = get_latest_file_path(path_to_save)

    # Определяем индекс последнего файла
    match = re.search(r'data_(\d+)_2025', last_file)

    if match:
        last_index = int(match.group(1))
    else:
        last_index = 0  # Если файлов нет, начинаем с 0

    # Создаем новый индекс
    new_index = last_index + 1
    current_time = datetime.now().strftime('%Y-%m-%d_%H-%M')

    # Создание имени файла с датой и временем
    file_name = f'parsed_data_{new_index}_{current_time}.xlsx'
    df.to_excel(os.path.join(path_to_save, file_name), index=False)

    print(f'Сохранен файл: {os.path.join(path_to_save, file_name)}')

# Пример вызова функции
# get_parser_data('URL_СТРАНИЦЫ', 'ПУТЬ_К_ПАПКЕ_ДЛЯ_СОХРАНЕНИЯ')