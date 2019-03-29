import requests
import asyncio
from fake_useragent import UserAgent
import functools
import json
from motor.motor_asyncio import AsyncIOMotorClient
import time

MONGO_HOST = "127.0.0.1"
MONGO_PORT = 27017
MONGO_DB = "sandroid_db"
MONGO_COL = "sandroid_col"


def mongo_conn(host, port, database, collection):
    conn = AsyncIOMotorClient(host, port)
    db = conn[database]
    col = db[collection]
    return col


class ApkInfoGetter(object):
    def __init__(self, md5list):
        self.list = []
        self.make_info_list(md5list)

    def make_info_list(self, md5list):
        url = 'http://sanddroid.xjtu.edu.cn/detail_report?apk_md5='
        for md5 in md5list:
            self.list.append(url + md5)


async def apk_info_crawler(dbcol, url):
    print('Start crawling: ', url)
    headers = {'user-agent': UserAgent().random}

    getter = asyncio.get_event_loop().run_in_executor(
        None,
        functools.partial(requests.post, url, data={}, headers=headers)
    )

    resp = await getter

    print('Response received: ', url)
    apk_info = await apk_info_builder(resp)
    if apk_info is not None:
        await insert_mongo(dbcol, apk_info)


async def apk_info_builder(response):
    try:
        apk_info_json = json.loads(response.text)
        apk_info_general = apk_info_json['general']
        apk_info_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(apk_info_general[1])))
        apk_info = {
            'MD5': apk_info_general[2],
            'Name': apk_info_general[5],
            'Time': apk_info_time
        }
    except json.JSONDecodeError:
        print(json.JSONDecodeError)
        return None
    print(apk_info)
    return apk_info


async def insert_mongo(dbcol, apk_info):
    dbcol.insert_one(apk_info)


def get_page(url, data):
    user_agent = {'user-agent': UserAgent().random}
    page = requests.post(url=url, data=data, headers=user_agent)
    if page.status_code == 200:
        return page
    else:
        print("Error: Loading Page...")
        return None


def get_md5_list():
    url = "http://sanddroid.xjtu.edu.cn/apk_table_info"
    data = {
        'sEcho': 1,
        'iColumns': 5,
        'sColumns': None,
        'iDisplayStart': 0,
        'iDisplayLength': 100,
        'mDataProp_0': 0,
        'mDataProp_1': 1,
        'mDataProp_2': 2,
        'mDataProp_3': 3,
        'mDataProp_4': 4,
        'iSortCol_0': 0,
        'sSortDir_0': 'asc',
        'iSortingCols': 1,
        'bSortable_0': 'true',
        'bSortable_1': 'true',
        'bSortable_2': 'true',
        'bSortable_3': 'true',
        'bSortable_4': 'true',
        'is_search': 'false'
    }
    md5_list = []

    while True:
        page = get_page(url=url, data=data)
        origin_json = json.loads(page.text)
        apk_list = origin_json['aaData']

        for apk in apk_list:
            if apk[-2] == 'UnDetected':
                continue
            else:
                md5_list.append(apk[1])

        yield md5_list

        data['iDisplayStart'] += 100
        data['iDisplayLength'] += 100
        md5_list = []


def main():

    md5_list = get_md5_list()
    db_conn = mongo_conn(MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COL)
    apk_count = 0

    loop = asyncio.get_event_loop()
    while True:
        next_md5_list = md5_list.__next__()
        apk_count += len(next_md5_list)
        apk_info_getter = ApkInfoGetter(next_md5_list)
        tasks = [apk_info_crawler(db_conn, url) for url in apk_info_getter.list]
        loop.run_until_complete(asyncio.wait(tasks))

        if apk_count >= 300:
            break
    print('\n\nAPK Count:', apk_count)
    loop.close()


if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    print("Use Time:", end_time - start_time)
