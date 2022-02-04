import csv
import threading
from threading import Thread
from time import sleep
import time

import requests
from bs4 import BeautifulSoup as BS

result = []
authors_set_list = set()
authors = []
start_time = time.time()


def add_authors(items, url, name):
    description = items.select('.author-description')[0].text
    authors.append({
        'name': name,
        'url': url,
        'born_date': items.select('.author-born-date')[0].text,
        'born_location': items.select('.author-born-location')[0].text,
        'description': description.replace('\n', ' ')
    })


def add_posts(items):
    for item in items:
        # Получение текста
        text = item.select('.text')[0].text

        # Получение автора
        author = {}
        author['name'] = item.select('.author')[0].text
        author['url'] = item.select('a')[0]['href']

        # Получение данных о пользователях
        if not (author['name'] in authors_set_list):
            authors_set_list.add(author['name'])
            response = requests.get(
                f"https://quotes.toscrape.com/{author['url']}/"
            )
            print(f"In process author page: {author['name']}")
            html = BS(response.content, 'html.parser')
            author_data = html.select('.container > .author-details')
            Thread(target=add_authors,
                   args=(author_data[0], author['url'], author['name'], )
                   ).start()

        # Получение тегов
        tags = []
        for tag in item.select('.tag'):
            tags.append({
                'name': tag.text,
                'url': tag['href']
            })

        # Объединение результатов
        result.append({
            'text': text,
            'author': author,
            'tags': tags
        })


def main():
    page = 1
    # запуск парсера
    while True:
        response = requests.get(f'https://quotes.toscrape.com/page/{page}/')
        print(f'In process page: {page}')
        html = BS(response.content, 'html.parser')
        items = html.select('.col-md-8 > .quote')
        if len(items):
            # Запуск потока по добовлению значений
            th = Thread(target=add_posts, args=(items,))
            th.start()
        else:
            # Ожидаем завершения
            while threading.active_count() > 1:
                sleep(0.1)
                print('wait')
            break
        page += 1

    finish_time = time.time() - start_time
    print(f"Затраченное на работу скрипта время: {finish_time}")

    # Выгрузка записей
    with open('posts.csv', mode='w', encoding='utf-8') as w_file:
        file_writer = csv.writer(w_file, delimiter=";", lineterminator="\r")
        file_writer.writerow(['author', 'text', 'tags'])
        for p in result:
            file_writer.writerow([p['author']['name'], p['text'],
                                 (', '.join([i['name'] for i in p['tags']]))])

    # Выгрузка пользователей (авторов)
    with open('users.csv', mode='w', encoding='utf-8') as w_file:
        file_writer = csv.writer(w_file, delimiter=";", lineterminator="\r")
        file_writer.writerow(['name', 'born_date', 'born_location',
                              'description'])
        for a in authors:
            file_writer.writerow([a['name'], a['born_date'],
                                  a['born_location'], a['description']])


if __name__ == "__main__":
    main()
