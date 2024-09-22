import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import time

user_agent = {'User-agent': 'Mozilla/5.0'}

# Функция для получения списка вакансий с API
def fetch_vacancies(page, user_agent):
    url = f'https://api.hh.ru/vacancies?text=middle python разработчик&page={page}&items_on_page=10&id=9'
    response = requests.get(url, headers=user_agent)
    return json.loads(response.text)

# Функция для получения данных о конкретной вакансии
def get_vacancy_details(vacancy_url, user_agent):
    response = requests.get(vacancy_url, headers=user_agent)
    return json.loads(response.text)

# Функция для обработки данных вакансии и очистки HTML
def process_vacancy_data(vacancy_data):
    # Извлечение ключевых навыков
    key_skills = vacancy_data.get('key_skills', [])
    skills = [skill['name'] for skill in key_skills]

    # Название вакансии
    name = vacancy_data.get('name', '')

    # Имя работодателя
    employer_name = vacancy_data.get('employer', {}).get('name', '')

    # Описание вакансии (очистка от HTML тегов)
    description_html = vacancy_data.get('description', '')
    soup = BeautifulSoup(description_html, 'html.parser')
    description = soup.get_text()

    return {
        'name': name,
        'employer_name': employer_name,
        'description': description,
        'skills': ', '.join(skills)
    }

# Функция для записи данных в базу данных
def save_vacancy_to_db(vacancy_data):
    # Создание DataFrame с данными
    df = pd.DataFrame([vacancy_data])

    # Подключение к базе данных SQLite
    conn = sqlite3.connect('vacancies2.db')

    # Запись DataFrame в базу данных
    df.to_sql('vacancies', conn, if_exists='append', index=False)

    # Закрытие соединения с базой данных
    conn.close()

# Основная функция для обработки страниц вакансий
def process_vacancy_pages(page_count):
    vac_count = 0

    for page in range(page_count):
        vacancies = fetch_vacancies(page, user_agent)

        for vacancy in vacancies['items']:
            vacancy_url = vacancy.get('url')
            vacancy_data = get_vacancy_details(vacancy_url, user_agent)

            if 'archived' in vacancy_data and not vacancy_data['archived']:
                processed_data = process_vacancy_data(vacancy_data)

                if processed_data['skills']:
                    save_vacancy_to_db(processed_data)

                    vac_count += 1
                    time.sleep(0.1)  # Задержка

                    print(f"Vacancy #{vac_count} saved")

# Вызов основной функции
process_vacancy_pages(20)
