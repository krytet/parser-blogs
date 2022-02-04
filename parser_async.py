import asyncio
import csv
import time

import aiohttp
from bs4 import BeautifulSoup as BS

result = []
authors_set_list = set()
authors = []
start_time = time.time()
tasks = []


async def add_authors(session, url, name):
    async with session.get(f'https://quotes.toscrape.com/{url}/',
                           ssl=False) as response:
        html = BS(await response.text(), 'html.parser')
        items = html.select('.container > .author-details')
        description = items[0].select('.author-description')[0].text
        authors.append({
            'name': name,
            'url': url,
            'born_date': items[0].select('.author-born-date')[0].text,
            'born_location': items[0].select('.author-born-location')[0].text,
            'description': description.replace('\n', ' ')
        })


async def add_posts(items, session):
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
            print(f"In process author page: {author['name']}")
            task = asyncio.create_task(add_authors(session, author['url'],
                                                   author['name'],))
            tasks.append(task)

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


async def gather_data():
    page = 1
    async with aiohttp.ClientSession() as session:
        while True:
            print(f'In process page: {page}')
            response = await session.get(
                url=f'https://quotes.toscrape.com/page/{page}/',
                ssl=False
            )
            html = BS(await response.text(), 'html.parser')
            items = html.select('.col-md-8 > .quote')
            if len(items):
                task = asyncio.create_task(add_posts(items, session))
                tasks.append(task)
            else:
                break
            page += 1
        await asyncio.gather(*tasks)


def main():
    # запуск парсера
    loop = asyncio.get_event_loop()
    loop.run_until_complete(gather_data())
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
