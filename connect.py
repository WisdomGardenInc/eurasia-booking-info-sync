import datetime
import json

import cx_Oracle
import requests
import time

from apscheduler.schedulers.blocking import BlockingScheduler

from config import cfg
from logger import log
from cacheout import Cache

s = requests.Session()
s.headers = {
    'X-Consumer-Custom-ID': cfg['sync']['org-code']
}
last_query_id = 0
date_format_pattern = "%Y-%m-%d"
cache = Cache(maxsize=256, ttl=3600 * 6, timer=time.time, default=None)
scheduler = BlockingScheduler()


def query_data_from_oracle():
    connect_str = "{}/{}@{}:1521/{}".format(cfg['sync']['oracle']['username'], cfg['sync']['oracle']['password'],
                                            cfg['sync']['oracle']['host'], cfg['sync']['oracle']['database'])
    connection = cx_Oracle.Connection(connect_str)
    cursor = connection.cursor()

    sql = """
            select
                ID as oracle_id,
                HYSBH as name,
                YYXGH as organizer_id,
                YYRQ as date,
                YYMC as topic,
                YYKSRQ as start_date,
                YYKSSJ as start_time,
                YYJSRQ as end_date,
                YYJSSJ as end_time,
                YYJYMS as descrption
            from
                ecology.uf_roomismeeting
            where
                id > :1
            and rownum <= :2
    """
    result = []
    try:
        cursor.execute(sql, (last_query_id, cfg['sync']['oracle']['page-size'],))
        for row in cursor:
            result.append(row)
    except IOError as e:
        log.error("execute error:", e)
    return result


def get_total_space():
    if len(cache.keys()) > 0:
        return
    spaces_url = cfg['sync']['url']['spaces']
    try:
        response = s.post(spaces_url).content.decode()
        spaces_result = json.loads(response)
        for space in spaces_result:
            cache.set(space['name'], space['id'])
    except requests.ConnectionError as e:
        log.error(e)
        return ''
    except requests.RequestException as e:
        log.error(e)
        return ''


def send_booking_info_to_roomis(data):
    if len(data) == 0:
        return
    global last_query_id
    last_query_id = int(data[-1]["oracle_id"])
    try:
        for element in data:
            space_id = cache.get(element['name'])
            if space_id is None:
                log.error("space with a name:{} is not exist in roomis!".format(element['name']))
                continue
            create_booking_param = {'organizerId': element['organizer_id'], 'date': element['date'],
                                    'description': element['description'], 'startDate': element['start_date'],
                                    'startTime': element['start_time'], 'externalId': element['oracle_id'] + "-FROM-OA"}
            start_date = datetime.datetime.strptime(element['start_date'], date_format_pattern)
            end_date = datetime.datetime.strptime(element['end_date'], date_format_pattern)
            request_create_booking_api(element, space_id, create_booking_param, start_date, end_date)
    except requests.ConnectionError as e:
        log.error(e)
    except requests.RequestException as e:
        log.error(e)


def request_create_booking_api(element, space_id, create_booking_param, start_date, end_date):
    differences = end_date - start_date
    booking_url = cfg['sync']['url']['booking'].format(space_id)
    for i in range(0, differences.days + 1):
        if i == 0 and differences.days == 0:
            create_booking_param['endDate'] = element['start_time']
            create_booking_param['endTime'] = element['end_time']
            s.post(booking_url, json.dumps(create_booking_param), timeout=30)
            continue
        current_date = start_date + datetime.timedelta(days=i)
        create_booking_param['date'] = current_date.date()
        if i == 0 and differences.days > 0:
            create_booking_param['endDate'] = element['start_date']
            create_booking_param['endTime'] = "23:59"
            s.post(booking_url, json.dumps(create_booking_param), timeout=30)
            continue
        if i == differences.days:
            create_booking_param['endDate'] = element['start_date']
            create_booking_param['endTime'] = "23:59"
            s.post(booking_url, json.dumps(create_booking_param), timeout=30)
            continue
        create_booking_param['startDate'] = current_date.date()
        create_booking_param['startTime'] = "00:00"
        create_booking_param['endDate'] = current_date.date()
        create_booking_param['endTime'] = "23:59"
        create_booking_param['allDay'] = True
        s.post(booking_url, json.dumps(create_booking_param), timeout=30)


def schedule_job():
    log.info("start schedule job")
    get_total_space()
    data = query_data_from_oracle()
    send_booking_info_to_roomis(data)


if __name__ == '__main__':
    scheduler.add_job(schedule_job, 'cron', hour='5-23', minute='0/1')
    scheduler.start()