import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import re  # Добавляем импорт библиотеки re для работы с регулярными выражениями
import os
from datetime import datetime
from utils import *

def parse_table(soup):
    # Найдите таблицу
    table = soup.find('table', {'class': 'table-border'})
    if not table:
        print("Таблица не найдена на странице.")
        return []  # Возвращаем пустой список, если таблица не найдена

    data = []
    for row in table.find_all('tr')[1:]:
        # Пропускаем заголовок
        cols = row.find_all('td')

        if len(cols) < 5:  # Убедитесь, что есть достаточно колонок
            continue  # Пропускаем, если строка неполная
        item = {
            'name': cols[0].text.strip(),
            'form': cols[1].text.strip(),
            'producer': cols[2].text.strip(),
            'price': cols[4].find(class_='price-value').text.strip(),
            'quantity': cols[4].find(class_='capture').text.strip()  # Извлекаем количество упаковок
        }
        data.append(item)

    return data


def clean_data(data):
    cleaned_data = []
    for item in data:
        name_parts = item['name'].split('\n')
        form_parts = item['form'].split('\n')
        producer_parts = item['producer'].split('\n')

        # Используем регулярное выражение для извлечения чисел (целых и дробных) из строки 'quantity'
        quantity_match = re.search(r'\d+\.?\d*', item['quantity'])
        only_quantity = quantity_match.group(0) if quantity_match else ''  # Извлекаем только числовое значение

        cleaned_item = {
            'name': name_parts[0],
            'item_type': name_parts[-1].strip() if len(name_parts) > 1 else '',
            'item_form': form_parts[0],
            'prescription': form_parts[-1].strip() if len(form_parts) > 1 else '',
            'manufacturer': producer_parts[0].strip(),
            'country': producer_parts[-1].strip() if len(producer_parts) > 1 else '',
            'price': item['price'],
            'quantity': item['quantity'],
            'only_quantity': only_quantity  # Заменяем на только числовое значение (целое или дробное)
        }
        cleaned_data.append(cleaned_item)

    return cleaned_data

def get_parser_data(url, path_to_save):

    # Настройка Selenium
    driver = webdriver.Chrome()  # Убедитесь, что chromedriver в PATH
    driver.get(url)

    carrets = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.select-check-carret'))
    )

    driver.execute_script("arguments[0].scrollIntoView(true);", carrets[1])
    driver.execute_script("arguments[0].click();", carrets[1])


    # Прокручиваем вниз перед выбором опции "Показывать по 100" и кликаем по ней
    option_100 = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//input[@value="100"]/following-sibling::label'))
    )

    driver.execute_script("arguments[0].scrollIntoView(true);", option_100)
    driver.execute_script("arguments[0].click();", option_100)

    all_data = []

    while True:
        time.sleep(random.uniform(1, 3))  # Ждем случайное время между запросами
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        page_data = parse_table(soup)

        if not page_data:
            print("Нет данных для отображения на этой странице.")
            break  # Выход из цикла, если данные отсутствуют

        all_data.extend(page_data)  # Добавляем данные текущей страницы в общий список
        print("Данные текущей страницы:")
        for item in page_data:
            print(item)

        # Пытаемся найти кнопку "Вперёд" и нажать на неё
        try:
            # Прокручиваем вниз перед нажатием на кнопку "Вперёд" и кликаем по ней
            button_next = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '.table-pagination-next a'))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", button_next)
            driver.execute_script("arguments[0].click();", button_next)
        except Exception as e:
            print("Ошибка при переходе на следующую страницу:", e)
            break

    # Закрываем браузер
    driver.quit()

    # Очистка данных
    cleaned_data = clean_data(all_data)

    # Создание DataFrame и запись в Excel
    # Предполагается, что cleaned_data уже определен
    df = pd.DataFrame(cleaned_data)

    # Получаем путь к последнему файлу
    last_file = get_latest_file_path(path_to_save)

    # Определяем индекс последнего файла
    if last_file:
        last_index = int(os.path.splitext(os.path.basename(last_file))[0].split('_')[
                             -1])  # Предполагается, что индекс находится в названии файла
    else:
        last_index = 0  # Если файлов нет, начинаем с 0

    # Создаем новый индекс
    new_index = last_index + 1

    current_time = datetime.now().strftime('%Y-%m-%d_%H-%M')

    # Создание имени файла с датой и временем
    file_name = f'parsed_data_{new_index}_{current_time}.xlsx'

    df.to_excel(f'{path_to_save}/parsed_data_{new_index}_{current_time}.xlsx', index=False)

    print(f'{path_to_save}/parsed_data_{new_index}_{current_time}.xlsx')