import os
import time
import functools
import asyncio
import shutil
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Value

import pandas as pd
import aiofiles
from bs4 import BeautifulSoup
from aiocsv import AsyncWriter


all_xml_dir = 'VO_OT3'
all_csv_dir = 'all_with_process_pool'

lst_xml = os.listdir(all_xml_dir)
len_files = len(lst_xml)

map_progress: Value


def init(progress):
    global map_progress
    map_progress = progress


def partition(data, qnt_files):
    for i in range(0, len(data), qnt_files):
        yield data[i:i + qnt_files]


def time_script(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.time()
        await func(*args, **kwargs)
        end = time.time()
        count_time = end - start
        return count_time
    return wrapper


async def iterator(lst_names_files):
    for file_name in lst_names_files:
        yield file_name
        await asyncio.sleep(0)


async def write_csv_one(lst_for_write_data_to_csv):
    with map_progress.get_lock():
        map_progress.value += 1
        count = map_progress.value

    name_csv = f'{all_csv_dir}/{count}.csv'

    async with aiofiles.open(name_csv, 'w') as file:
        writer = AsyncWriter(file)
        await writer.writerow(
            (
                'ИНН',
                'Количество сотрудников',
                'Год'
            )
        )
        await writer.writerows(lst_for_write_data_to_csv)

    return name_csv, count


async def read_xml(file_name):
    async with aiofiles.open(f'{all_xml_dir}/{file_name}') as file:
        src = await file.read()
        return src


async def find_one(items):
    lst_for_write_data_to_csv = []

    for item in items:
        inn = item.find('СведНП').get('ИННЮЛ')
        count_sot = item.find('СведССЧР').get('КолРаб')
        date_sost = item.get('ДатаСост').split('.')[-1]
        lst_for_write_data_to_csv.append([inn, count_sot, date_sost])
    return lst_for_write_data_to_csv


async def parse_one(files_names):
    tasks = (read_xml(file_name) for file_name in files_names)
    all_src = await asyncio.gather(*tasks)

    all_soup = (BeautifulSoup(src, features='xml') for src in all_src)
    all_items = (soup.find_all('Документ') for soup in all_soup)

    tasks_all_data = (find_one(items) for items in all_items)
    all_data = await asyncio.gather(*tasks_all_data)

    tasks_all_writer = [write_csv_one(data) for data in all_data]

    files_names_for_join_csw = []

    for one_res in asyncio.as_completed(tasks_all_writer):
        file_name, count = await one_res
        files_names_for_join_csw.append(file_name)
        print(f'Обработано файлов {count} из {len_files}')

    return files_names_for_join_csw


def main_in_one_process(files_names):
    files_names_for_join_csw = asyncio.run(parse_one(files_names))

    return files_names_for_join_csw


@time_script
async def main(qnt_files):
    loop = asyncio.get_running_loop()
    tasks = []
    count = Value('i', 0)

    with ProcessPoolExecutor(max_workers=os.cpu_count(), initializer=init, initargs=(count,)) as executor:
        for file_names in partition(lst_xml, qnt_files):
            tasks.append(loop.run_in_executor(executor, functools.partial(main_in_one_process, file_names)))

        lst_for_join_df = []

        for item in asyncio.as_completed(tasks):
            res = await item
            part_df = pd.concat(pd.read_csv(file_name) for file_name in res)
            lst_for_join_df.append(part_df)

        joined_df = pd.concat(df for df in lst_for_join_df)

        joined_df.to_csv('result.csv', index=False)
        print(joined_df.groupby(['Год', 'ИНН'])['Количество сотрудников'].sum())


if __name__ == '__main__':
    if not os.path.exists(all_csv_dir):
        os.mkdir(all_csv_dir)

    res_time = asyncio.run(main(50))
    print(f'Скрипт завершил работу за {res_time} сек.')

    if os.path.exists(all_csv_dir):
        shutil.rmtree(all_csv_dir)
