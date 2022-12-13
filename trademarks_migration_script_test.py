# This Python file uses the following encoding: utf-8
import uuid
import os
import re
import shutil
import sys
import datetime
import time
import psycopg2
from psycopg2 import extras
from dbfread import DBF
from PIL import Image, UnidentifiedImageError
import logging
from logging.handlers import RotatingFileHandler


NULL_VALUE = None


TIME_FORMAT = "%Y-%m-%d %H:%M:%S" # задание формата для базы данных
DESTINATION_DIRECTORY = 'C:\\astz_images\\' # взятие файла из дериктории 
NFS = '/mnt/nfs/data/datamart/'
CONSOLE_HANDLER = logging.StreamHandler()# инициирования класса StreamHandler для того чтобы читать или выводить жуурнал вызовов
LOG_HANDLER = RotatingFileHandler(
    'trademark_logs\\{}.log'.format(datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")),# инициилизация класса RotatingFileHandler озвращает новый экземпляр класса RotatingFileHandler. Указанный файл filename открывается и используется в качестве потока для ведения журнала
    mode='a', maxBytes=50*1024*1024, 
    backupCount=100, encoding='utf-8', delay=0)
logging.basicConfig(handlers=(LOG_HANDLER, CONSOLE_HANDLER),
                    format='[%(asctime)s.%(msecs)03d | %(levelname)s]: %(message)s',
                    datefmt=TIME_FORMAT,
                    level=logging.INFO)# выполняет базовую настройку системы ведения журнала 
ERROR_STRING = '\033[31m Ошибка {} в файле {} в строке {}: {} \033[0m'

PRIORITY_DICT = {
    'DAPK': '(320) подачи первой (первых) заявки (заявок) в государстве - участнике Парижской конвенции по охране промышленной собственности (пункта 1 статьи 1495 Гражданского кодекса Российской Федерации (Собрание законодательства Российской Федерации, 2006, № 52, ст. 5496) (далее – Кодекс)',
    'DAPV': '(230) начала открытого показа экспоната на выставке (пункт 2 статьи 1495 Кодекса)',
    'DFAP': '(641) приоритета первоначальной заявки, из которой данная заявка выделена (пункт 2 статьи 1494 Кодекса)',
    'UNKNOWN': '<ТИП ПРИОРИТЕТА НЕИЗВЕСТЕН>'
}

def time_test(func):
    """Функция декоратор, измеряет время выполнения функций."""
    def f(*args):
        t1 = time.time()
        res = func(*args)
        t2 = time.time()
        result = round(int(t2 - t1) / 60, 3)
        logging.info('Время выполнения функции {}: {} минут'.format(func.__name__, result))
        return res
    return f


@time_test
def connect_to_database(): # функция для соедениние с базой данных postgreesql
    connection = psycopg2.connect(dbname='uad_dev', user='uad',
                              password='*****', host='*****', port=00000)
    logging.info('Соединение установлено')
    cursor = connection.cursor()# устоновка там где был использован последний  файл  
    return connection, cursor

CONNECTION, CURSOR = connect_to_database()


@time_test
def get_storage_obj(connection, cursor, received_date, parent_number=None): # функция складиролвание объектов объекты складируются там где был устоновлен последний курсор
    """Создает новый объект хранения (корневой или вложенный)."""
    storage_obj_uuid = uuid.uuid1()
    if parent_number == None:
        kind = 100001  
        parent_uid = 'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF'
    else:
        kind = 150003
    update_date = datetime.datetime.now().strftime(TIME_FORMAT)
    created_date = datetime.datetime.now().strftime(TIME_FORMAT)
    state = 0
    oper_storage_period = datetime.datetime.now().strftime(TIME_FORMAT)
    temp_storage_period = datetime.datetime.now().strftime(TIME_FORMAT)
    class_type = 0
    last_storage_period = "0001-01-01 00:00:00"
    version = 1
    structure_id = 100
    try:
        add_storage_obj = """
            INSERT INTO "Objects" ("Number", "UpdateDate", "CreatedDate", "State", "OperStoragePeriod", "TempStoragePeriod", "ClassType", "LastStoragePeriod", "Version", "Kind", "ParentNumber", "StructureID", "Received") VALUES ('{}', '{}', '{}', {}, '{}', '{}', {}, '{}', {}, {}, '{}', {}, '{}')
        """.format(str(storage_obj_uuid), update_date, created_date, state, oper_storage_period, temp_storage_period,
                   class_type, last_storage_period, version, kind, parent_number if parent_number else parent_uid,
                   structure_id, received_date if received_date else None)
        cursor.execute(add_storage_obj)
        connection.commit()
        logging.info("Создан ОХ с номером %s", storage_obj_uuid)
        return storage_obj_uuid
    except Exception as error:
        exc_type, _, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error('Ошибка {} в файле {}'.format(exc_type, file_name),
                     'в строке {}: {}'.format(exc_tb.tb_lineno, error))
        # эта функция нужна для обновление базы данных и создание новых записей


@time_test
def collect_data(directory):
    """Создает из выгрузки словарь для дальнейшей обработки"""
    try:
        goods_name_template = 'MD_GOOD{}.DBF'
        mains_path = os.path.join(directory, "MD_MAINS.DBF")
        big_dict = {r['NSER']: dict(r) for r in sorted(DBF(mains_path, ignore_missing_memofile=True), key=lambda x: x['NSER']) if (r['NTM'][:3] != '999' and r['IS'] != 'I' and r['WCD'] != 'N')}
        file_types = {'TIF': 'TIFF',
                      'JPG': 'JPEG'}
        for num in range(9):
            dbf_name = goods_name_template.format("S" if num == 0 else str(num))
            dbf_path = os.path.join(
                directory, dbf_name)
            if os.path.isfile(dbf_path):
                goods = [r for r in DBF(dbf_path, ignore_missing_memofile=True)]
                for r in goods:
                    try:
                        big_dict[r['NSER']]['GOODS'] = r['GOODS']
                    except KeyError:
                        print('Такого ключа нет в основном словаре.')
                        continue
        for root, _, files in os.walk(directory + '/IMG/'):
            for file in files:
                if '.TIF' in file:
                    image_path = os.path.join(root, file)
                    name, extension = file.split('.')
                    try:
                        big_dict[int(name)]['IMAGE_PATH'] = image_path
                        big_dict[int(name)]['IMAGE_NAME'] = name
                        big_dict[int(name)]['IMAGE_TYPE'] = file_types[extension]
                    except KeyError:
                        print('Такого ключа нет в основном словаре.')
    except Exception as ex:
        exc_type, _, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(ERROR_STRING.format(exc_type, file_name, exc_tb.tb_lineno, ex))
    return big_dict
# судя по всему эта функция нужна для того чтобы сортировать и складировать данные товаров


@time_test
def get_record_dict(dictionary):# получение записчей из словаря ))) 
    record_dictionary = {}

    for key, value in dictionary.items():
        if type(value) == str:
            if "'" in value:
                        wrong_value = value.split("'")
                        value = "`".join(wrong_value)
        if key == 'NSER':
            record_dictionary['retro_number'] = value
        elif key == 'NAP':
            record_dictionary['appl_number'] = value
        elif key == 'DAP':
            record_dictionary['appl_date'] = value
        elif key == 'CU':
            record_dictionary['reg_country'] = value
            record_dictionary['country_code'] = value
            record_dictionary['nationality_code'] = value
        elif key == 'NTM':
            record_dictionary['reg_number'] = value
        elif key == 'DPUB':
            record_dictionary['reg_date'] = value
        elif key == 'DEX':
            record_dictionary['expiry_date'] = value
        elif key == 'SDACT':
            record_dictionary['status_code'] = value
        elif key == 'CFAP':
            record_dictionary['priority_country'] = value
        elif key == 'DFAP':
            record_dictionary['first_priority_date'] = value
        elif key == 'DAPK':
            record_dictionary['conven_priority_date'] = value
        elif key == 'DAPV':
            record_dictionary['exhib_priority_date'] = value
        elif key == 'OKPO':
            record_dictionary['employee_number'] = value
        elif key == 'WWT':
            record_dictionary['representation_sign'] = value
        elif key =='GS':
            record_dictionary['goods'] = value
        elif key == 'OWN2':
            record_dictionary['applicants'] = value
            record_dictionary['holders'] = value
            record_dictionary['name_holder_applicant'] = value
            record_dictionary['corr_adressee'] = value
        elif key == 'OWNS':
            record_dictionary['incorporation'] = value
        elif key == 'MAIL':
            record_dictionary['contact_address_text'] = value
        elif key == 'MAIL2':
            record_dictionary['corresp_address'] = value
        elif key == 'M_INDEX':
            record_dictionary['postal_code'] = value
        elif key == 'M_SUBC':
            record_dictionary['province_name'] = value
        elif key == 'M_SUBJ':
            record_dictionary['region_name'] = value
        elif key == 'M_CITY':
            record_dictionary['city_name'] = value
        elif key == 'SDIZM':
            record_dictionary['status_date'] = value
        elif key == 'KPP':
            record_dictionary['representative_number'] = value
        elif key == 'NPP':
            record_dictionary['name_representative'] = value
        elif key == 'IMAGE_NAME':
            record_dictionary['representation_names'] = value
            record_dictionary['image_name'] = value
        elif key == 'IMAGE_PATH':
            record_dictionary['image_path'] = value
        elif key == 'IMAGE_TYPE':
            record_dictionary['image_type'] = value
        elif key == 'GOODS':
            record_dictionary['goods_services'] = value
    return record_dictionary


def delete_commas(start_string): # удалеие запитых в записи
    if start_string.startswith(','):
        start_string = start_string.replace(',', '')
    interim_string = start_string.replace('-', '')
    final_string = interim_string.strip()
    return final_string


@time_test
def create_rutrademark(CONNECTION, CURSOR, record_dictionary, root_object_uuid, rutmk_uid_value, update_time, NULL_VALUE): # создание торговой марки 
    if record_dictionary['appl_date'] is not None and record_dictionary['appl_date'] != 'None':
        application_date = record_dictionary['appl_date']
    else:
        application_date = ''

    # priority dates
    priority_dates = []
    if record_dictionary.get('conven_priority_date') is not None:
        priority_dates.append(record_dictionary['conven_priority_date'])
    if record_dictionary.get('exhib_priority_date') is not None:
        priority_dates.append(record_dictionary['exhib_priority_date'])
    if record_dictionary.get('first_priority_date') is not None:
        priority_dates.append(record_dictionary['first_priority_date'])
    if priority_dates != []:
        pr_dates = ';'.join(priority_dates)
        formatted_priority_date = '{' + pr_dates + '}'
    else:
        dummy_date = datetime.date(1, 1, 1) 
        formatted_priority_date = '{' + dummy_date + '}'

    if record_dictionary.get('representation_names') is not None:
        representation_names = record_dictionary['representation_names']
    else:
        representation_names = ''

    #goods_classes
    goods = record_dictionary['goods'].split() if record_dictionary['goods'] is not None and record_dictionary['goods'] != 'None' else ''
    if goods != '':
        goods_classes = '; '.join(goods)
    else:
        goods_classes = ''

    #goods_text
    goods_services = record_dictionary['goods_services']
    
    #is_external_search
    if record_dictionary['reg_date'] is not None and record_dictionary['reg_date'] != 'None':
        if record_dictionary['reg_date'] >= datetime.date(2014, 10, 25):
            is_external_search = 1
        else:
            is_external_search = 0
    else:
        is_external_search = 0

    values_list = [str(rutmk_uid_value),
        record_dictionary['retro_number'],
        '', #appl_doc_link
        '', #appl_ui_link
        '', #appl_type
        '', #appl_receiving_date
        record_dictionary['appl_number'],
        application_date,
        '', #reg_ui_link
        '', #reg_doc_link
        record_dictionary['reg_number'],
        record_dictionary['reg_date'],
        record_dictionary['reg_country'],
        '', #reg_publ_number
        '', #reg_publ_date
        record_dictionary['status_code'],
        record_dictionary['status_date'],
        record_dictionary['expiry_date'],
        formatted_priority_date,
        '', #other_date
        record_dictionary['corresp_address'],
        '', #corr_address_country
        record_dictionary['applicants'],
        1, #applicants_count
        record_dictionary['holders'],
        1, #holders_count
        record_dictionary['name_representative'],
        '', #representatives_type
        1, #representatives_count
        record_dictionary['representative_number'] if record_dictionary['representative_number'] is not None and record_dictionary['representative_number'] != 'None' else '',
        '', #representatives_term
        '', #users
        0, #users_count
        '', #mark_category
        representation_names,
        '', #search_result
        goods_classes,
        goods_services,
        '', #prev_reg_number
        '', #prev_reg_date
        '', #prev_reg_country
        '', #feature_description
        '', #disclamers
        '', #association_marks
        '', #payment
        '', #records
        '', #corr_type
        '', #corr_method
        '', #sheets_count
        '', #image_sheets_count
        '', #payment_doc_count
        is_external_search, #TODO - нужно подтверждение О. Федосеевой
        update_time,
        '', #delete_time
        str(root_object_uuid)]
    formatted_values = []
    for val in values_list:
        if type(val) == None:
            formatted_values.append('NULL')
        elif type(val) == datetime.datetime:
            val = val.strftime("%Y.%m.%d %H:%M:%S")
            formatted_values.append(val)
        elif type(val) == datetime.date:
            val = val.strftime("%Y.%m.%d")
            formatted_values.append(val)
        elif type(val) == int:
            val = str(val)
            formatted_values.append(val)
        elif type(val) == uuid:
            val = str(val)
            formatted_values.append(val)
        elif val == '':
            formatted_values.append('NULL')
        elif type(val) == str:
            val = val.replace("\"", "")
            filtered_val = list(s for s in val if s.isprintable())
            val = ''.join(filtered_val)
            print(repr(val))
            formatted_values.append(val)
        else:
            formatted_values.append(val)
    
    values = ', '.join(["'" + str(i) + "'" if i else 'NULL' for i in formatted_values])
    values = values.replace('\'NULL\'', 'NULL')
    print(values)

    ready_query = "INSERT INTO fips_rutrademark (rutmk_uid, retro_number, appl_doc_link, appl_ui_link, appl_type, appl_receiving_date, appl_number, appl_date, reg_ui_link, reg_doc_link, reg_number, reg_date, reg_country, reg_publ_number, reg_publ_date, status_code, status_date, expiry_date, priority_date, other_date, corr_address, corr_address_country, applicants, applicants_count, holders, holders_count, representatives, representatives_type, representatives_count, representative_number, representatives_term, users, users_count, mark_category, representation_names, search_result, goods_classes, goods, prev_reg_number, prev_reg_date, prev_reg_country, feature_description, disclaimers, association_marks, payment, records, corr_type, corr_method, sheets_count, image_sheets_count, payment_doc_count, is_external_search, update_time, delete_time, object_uid) VALUES({})".format(values)

    print(repr(ready_query))
    try:
        CURSOR.execute(ready_query)
        CONNECTION.commit()
        logging.info("Создана запись в таблице fips_rutrademark для NSER = {}".format(record_dictionary['retro_number']))
    except Exception as error:
        logging.error("Ошибка создания записи в таблице fips_rutrademark для NSER = {}: {}".format(record_dictionary['retro_number'], error))


def create_rutmkpriority(CONNECTION, CURSOR, record_dictionary, rutmk_uid_value, priority_type, priority_date, priority_appl_number, priority_appl_date): # создание приоретета марок

    tmk_priority_uid_value = uuid.uuid1()

    values_list = [
        str(tmk_priority_uid_value),
        str(rutmk_uid_value),
        record_dictionary['priority_country'] if record_dictionary['priority_country'] is not None and record_dictionary['priority_country'] != 'None' else '',
        priority_type,
        priority_date,
        '', # priority_appl_number
        '', #priority_appl_date,
        '' # priority_status
    ]
    formatted_values = []
    for val in values_list:
        if type(val) == None:
            formatted_values.append('NULL')
        elif type(val) == datetime.datetime:
            val = val.strftime("%Y.%m.%d %H:%M:%S")
            formatted_values.append(val)
        elif type(val) == datetime.date:
            val = val.strftime("%Y.%m.%d")
            formatted_values.append(val)
        elif type(val) == int:
            val = str(val)
            formatted_values.append(val)
        elif type(val) == uuid:
            val = str(val)
            formatted_values.append(val)
        elif val == '':
            formatted_values.append('NULL')
        elif type(val) == str:
            val = val.replace("\"", "")
            filtered_val = list(s for s in val if s.isprintable())
            val = ''.join(filtered_val)
            print(repr(val))
            formatted_values.append(val)
        else:
            formatted_values.append(val)

    values = ', '.join(["'" + str(i) + "'" if i else 'NULL' for i in formatted_values])
    values = values.replace('\'NULL\'', 'NULL')
    rutmkpriority_insert_query = 'INSERT INTO "fips_rutmkpriority" (tmk_priority_uid, rutmk_uid, priority_country, priority_type, priority_date, priority_appl_number, priority_appl_date, priority_status) VALUES({})'.format(values)
    print(repr(rutmkpriority_insert_query))
    try:
        CURSOR.execute(rutmkpriority_insert_query)
        CONNECTION.commit()
        logging.info("Создана запись в таблице fips_rutmkpriority для NSER = {}".format(record_dictionary['retro_number']))
    except Exception as error:
        logging.error("Ошибка создания записи в таблице fips_rutmkpriority для NSER = {}: {}".format(record_dictionary['retro_number'], error))
    # и добовление их в базу данных

def create_correspondenceaddress(CONNECTION, CURSOR, record_dictionary, address_uid_value, update_time): # создание корреспонденцкого адреса
    values_list = [
        str(address_uid_value),
        record_dictionary['corresp_address'],
        'ru',
        record_dictionary['country_code'] if record_dictionary['country_code'] is not None else 'Не указан',
        record_dictionary['postal_code'] if record_dictionary['postal_code'] is not None else '',
        record_dictionary['region_name'] if record_dictionary['region_name'] is not None else '',
        record_dictionary['province_name'] if record_dictionary['province_name'] is not None else '',
        record_dictionary['city_name'] if record_dictionary['city_name'] is not None else '',
        '', # address_line - нет данных для заполнения
        record_dictionary['corr_adressee'] if record_dictionary['corr_adressee'] is not None else '',
        update_time,
        ''
    ]
    formatted_values = []
    for val in values_list:
        if type(val) == None:
            formatted_values.append('NULL')
        elif type(val) == datetime.datetime:
            val = val.strftime("%Y.%m.%d %H:%M:%S")
            formatted_values.append(val)
        elif type(val) == datetime.date:
            val = val.strftime("%Y.%m.%d")
            formatted_values.append(val)
        elif type(val) == int:
            val = str(val)
            formatted_values.append(val)
        elif type(val) == uuid:
            val = str(val)
            formatted_values.append(val)
        elif val == '':
            formatted_values.append('NULL')
        elif type(val) == str:
            val = val.replace("\"", "")
            filtered_val = list(s for s in val if s.isprintable())
            val = ''.join(filtered_val)
            print(repr(val))
            formatted_values.append(val)
        else:
            formatted_values.append(val)

    values = ', '.join(["'" + str(i) + "'" if i else 'NULL' for i in formatted_values])
    values = values.replace('\'NULL\'', 'NULL')
    print(values)

    correspondenceaddress_insert_query = "INSERT INTO fips_correspondenceaddress (cor_address_uid, address_text, language_code, country_code, postal_code, region_name, province_name, city_name, address_line, addressee, update_time, delete_time) VALUES({})".format(values)

    print(repr(correspondenceaddress_insert_query))

    try:
        CURSOR.execute(correspondenceaddress_insert_query)
        CONNECTION.commit()
        logging.info("Создана запись в таблице fips_correspondenceaddress для NSER = {}".format(record_dictionary['retro_number']))
    except Exception as error:
        logging.error("Ошибка создания записи в таблице fips_correspondenceaddress для NSER = {}: {}".format(record_dictionary['retro_number'], error))


def create_rutmkcorrespondenceaddress(CONNECTION, CURSOR, record_dictionary, rutmk_uid_value, address_uid_value):# создание марки коресподенсткого адреса
    rutmkcorrespondenceaddress_insert_query = """INSERT INTO "fips_rutmkcorrespondenceaddress"
    (
        address_uid,
        rutmk_uid
    )
    VALUES ('{}', '{}') """.format(str(address_uid_value), str(rutmk_uid_value))
    try:
        CURSOR.execute(rutmkcorrespondenceaddress_insert_query)
        CONNECTION.commit()
        logging.info("Создана запись в таблице fips_rutmkcorrespondenceaddress для NSER = {}".format(record_dictionary['retro_number']))
    except Exception as error:
        logging.error("Ошибка создания записи в таблице fips_correspondenceaddress для NSER = {}: {}".format(record_dictionary['retro_number'], error))


def create_rutmkgoodsservices(CONNECTION, CURSOR, record_dictionary, rutmk_uid_value, class_number, class_definition): # создание марки товаров и услуг

    tmk_gs_uid_value = uuid.uuid1()

    #classifier_version
    identifier_check = record_dictionary['appl_date']
    if identifier_check is not None and identifier_check != 'None':
        if datetime.date(1963, 1, 1) <= identifier_check <= datetime.date(1970, 12, 31):
            classifier_version = 1
        elif datetime.date(1971, 1, 1) <= identifier_check <= datetime.date(1980, 12, 31):
            classifier_version = 2
        elif datetime.date(1981, 1, 1) <= identifier_check <= datetime.date(1982, 12, 31):
            classifier_version = 3
        elif datetime.date(1983, 1, 1) <= identifier_check <= datetime.date(1986, 12, 31):
            classifier_version = 4
        elif datetime.date(1987, 1, 1) <= identifier_check <= datetime.date(1991, 12, 31):
            classifier_version = 5
        elif datetime.date(1992, 1, 1) <= identifier_check <= datetime.date(1996, 12, 31):
            classifier_version = 6
        elif datetime.date(1997, 1, 1) <= identifier_check <= datetime.date(2001, 12, 31):
            classifier_version = 7
        elif datetime.date(2002, 1, 1) <= identifier_check <= datetime.date(2006, 12, 31):
            classifier_version = 8
        elif datetime.date(2007, 1, 1) <= identifier_check <= datetime.date(2011, 12, 31):
            classifier_version = 9
        elif datetime.date(2012, 1, 1) <= identifier_check <= datetime.date(2016, 12, 31):
            classifier_version = 10
        elif identifier_check >= datetime.date(2017, 1, 1):
            classifier_version = 11
    else:
        classifier_version = 'Не указано'

    values_list = [
        str(tmk_gs_uid_value),
        str(rutmk_uid_value),
        classifier_version,
        class_number,
        class_definition,
        'ru'
    ]
    formatted_values = []
    for val in values_list:
        if type(val) == None:
            formatted_values.append('NULL')
        elif type(val) == datetime.datetime:
            val = val.strftime("%Y.%m.%d %H:%M:%S")
            formatted_values.append(val)
        elif type(val) == datetime.date:
            val = val.strftime("%Y.%m.%d")
            formatted_values.append(val)
        elif type(val) == int:
            val = str(val)
            formatted_values.append(val)
        elif type(val) == uuid:
            val = str(val)
            formatted_values.append(val)
        elif val == '':
            formatted_values.append('NULL')
        elif type(val) == str:
            val = val.replace("\"", "")
            filtered_val = list(s for s in val if s.isprintable())
            val = ''.join(filtered_val)
            print(repr(val))
            formatted_values.append(val)
        else:
            formatted_values.append(val)

    values = ', '.join(["'" + str(i) + "'" if i else 'NULL' for i in formatted_values])
    values = values.replace('\'NULL\'', 'NULL')
    print(values)

    rutmkgoodsservices_insert_query = "INSERT INTO fips_rutmkgoodsservices (tmk_gs_uid, rutmk_uid, classifier_version, goods_class, definition, definition_lang_code) VALUES({})".format(values)
    print(repr(rutmkgoodsservices_insert_query))

    try:
        CURSOR.execute(rutmkgoodsservices_insert_query)
        CONNECTION.commit()
        logging.info("Создана запись в таблице fips_rutmkgoodsservices для NSER = {}".format(record_dictionary['retro_number']))
    except Exception as error:
        logging.error("Ошибка создания записи в таблице fips_rutmkgoodsservices для NSER = {}: {}".format(record_dictionary['retro_number'], error))


def create_contact(CONNECTION, CURSOR, record_dictionary, update_time, contact_uid_value, mode=None): # создание контакта владельца марки и добовление его в базу данных
    values_list = [
        str(contact_uid_value),
        'ru',
        record_dictionary['name_holder_applicant'] if mode == 'holder_or_applicant' else record_dictionary[
            'name_representative'],
        '',  # name_translit
        record_dictionary['nationality_code'] if mode == 'holder_or_applicant' else '',
        record_dictionary['contact_address_text'] if mode == 'holder_or_applicant' else '',
        record_dictionary['country_code'] if mode == 'holder_or_applicant' else 'Не указан',
        record_dictionary['postal_code'] if mode == 'holder_or_applicant' else '',
        record_dictionary['region_name'] if mode == 'holder_or_applicant' else '',
        record_dictionary['province_name'] if mode == 'holder_or_applicant' else '',
        record_dictionary['city_name'] if mode == 'holder_or_applicant' else '',
        record_dictionary['incorporation'] if record_dictionary['incorporation'] is not None and record_dictionary['incorporation'] != 'None' else '',
        record_dictionary['employee_number'] if mode == 'holder_or_applicant' else '',
        update_time
    ]
    formatted_values = []
    for val in values_list:
        if type(val) == None:
            formatted_values.append('NULL')
        elif type(val) == datetime.datetime:
            val = val.strftime("%Y.%m.%d %H:%M:%S")
            formatted_values.append(val)
        elif type(val) == datetime.date:
            val = val.strftime("%Y.%m.%d")
            formatted_values.append(val)
        elif type(val) == int:
            val = str(val)
            formatted_values.append(val)
        elif type(val) == uuid:
            val = str(val)
            formatted_values.append(val)
        elif val == '':
            formatted_values.append('NULL')
        elif type(val) == str:
            val = val.replace("\"", "")
            filtered_val = list(s for s in val if s.isprintable())
            val = ''.join(filtered_val)
            print(repr(val))
            formatted_values.append(val)
        else:
            formatted_values.append(val)

    values = ', '.join(["'" + str(i) + "'" if i else 'NULL' for i in formatted_values])
    values = values.replace('\'NULL\'', 'NULL')
    print(values)

    contact_insert_query = "INSERT INTO fips_contact (contact_uid, language_code, name, name_translit, nationality_code, address_text, country_code, postal_code, region_name,         province_name, city_name, incorporation, employee_number, update_time) VALUES({})".format(values)
    print(repr(contact_insert_query))

    try:
        CURSOR.execute(contact_insert_query)
        CONNECTION.commit()
        logging.info("Создана запись в таблице fips_contact для NSER = {}".format(record_dictionary['retro_number']))
    except Exception as error:
        logging.error("Ошибка создания записи в таблице fips_contact для NSER = {}: {}".format(record_dictionary['retro_number'], error))


def create_rutmkapplicant(CONNECTION, CURSOR, contact_uid, rutmk_uid, record_dictionary): # создание контакта марки заявителя

    rutmkapplicant_insert_query = """INSERT INTO "fips_rutmkapplicant" 
    (
        contact_uid,
        rutmk_uid
    )
    VALUES ('{}', '{}')""".format(str(contact_uid), str(rutmk_uid))
    try:
        CURSOR.execute(rutmkapplicant_insert_query)
        CONNECTION.commit()
        logging.info("Создана запись в таблице fips_rutmkapplicant для NSER = {}".format(record_dictionary['retro_number']))
    except Exception as error:
        logging.error("Ошибка создания записи в таблице fips_rutmkapplicant для NSER = {}: {}".format(record_dictionary['retro_number'], error))


def create_rutmkholder(CONNECTION, CURSOR, contact_uid, rutmk_uid, record_dictionary): # создание контакта держатилея марки

    rutmkholder_insert_query = """INSERT INTO "fips_rutmkholder"
    (
        contact_uid,
        rutmk_uid
    )
    VALUES ('{}', '{}') """.format(str(contact_uid), str(rutmk_uid))
    try:
        CURSOR.execute(rutmkholder_insert_query)
        CONNECTION.commit()
        logging.info("Создана запись в таблице fips_rutmkholder для NSER = %s".format(record_dictionary['retro_number']))
    except Exception as error:
        logging.error("Ошибка создания записи в таблице fips_rutmkholder для NSER = %s: %s".format(record_dictionary['retro_number'], error))


def create_rutmkrepresentative(CONNECTION, CURSOR, contact_uid, rutmk_uid, record_dictionary):# создание представителя марки

    rutmkrepresentative_insert_query = """INSERT INTO "fips_rutmkrepresentative" 
    (
        contact_uid,
        rutmk_uid
    )
    VALUES ('{}', '{}')""".format(str(contact_uid), str(rutmk_uid))
    try:
        CURSOR.execute(rutmkrepresentative_insert_query)
        CONNECTION.commit()
        logging.info("Создана запись в таблице fips_rutmkrepresentative для NSER = {}".format(record_dictionary['retro_number']))
    except Exception as error:
        logging.error("Ошибка создания записи в таблице fips_rutmkrepresentative для NSER = {}: {}".format(record_dictionary['retro_number'], error))


def create_contactaddress(CONNECTION, CURSOR, contact_uid_value, record_dictionary, update_time, mode=None): # создание контактного адреса
    contact_address_uid_value = uuid.uuid1()
    values_list = [
        str(contact_address_uid_value),
        str(contact_uid_value),
        'ru',  # language_code
        record_dictionary['contact_address_text'] if mode == 'holder_or_applicant' else '',
        record_dictionary['country_code'] if mode == 'holder_or_applicant' else 'Не указан',
        record_dictionary['postal_code'] if mode == 'holder_or_applicant' else '',
        record_dictionary['region_name'] if mode == 'holder_or_applicant' else '',
        record_dictionary['province_name'] if mode == 'holder_or_applicant' else '',
        record_dictionary['city_name'] if mode == 'holder_or_applicant' else '',
        record_dictionary['name_holder_applicant'] if mode == 'holder_or_applicant' else record_dictionary[
            'name_representative'],
        update_time
    ]
    formatted_values = []
    for val in values_list:
        if type(val) == None:
            formatted_values.append('NULL')
        elif type(val) == datetime.datetime:
            val = val.strftime("%Y.%m.%d %H:%M:%S")
            formatted_values.append(val)
        elif type(val) == datetime.date:
            val = val.strftime("%Y.%m.%d")
            formatted_values.append(val)
        elif type(val) == int:
            val = str(val)
            formatted_values.append(val)
        elif type(val) == uuid:
            val = str(val)
            formatted_values.append(val)
        elif val == '':
            formatted_values.append('NULL')
        elif type(val) == str:
            val = val.replace("\"", "")
            filtered_val = list(s for s in val if s.isprintable())
            val = ''.join(filtered_val)
            print(repr(val))
            formatted_values.append(val)
        else:
            formatted_values.append(val)

    values = ', '.join(["'" + str(i) + "'" if i else 'NULL' for i in formatted_values])
    values = values.replace('\'NULL\'', 'NULL')
    print(values)

    contactaddress_insert_query = "INSERT INTO fips_contactaddress (address_uid, contact_uid, language_code, address_text, country_code, postal_code, region_name, province_name, city_name, addressee, update_time) VALUES({})".format(values)
    print(repr(contactaddress_insert_query))

    try:
        CURSOR.execute(contactaddress_insert_query)
        CONNECTION.commit()
        logging.info("Создана запись в таблице fips_contactaddress для NSER = {}".format(record_dictionary['retro_number']))
    except Exception as error:
        logging.error("Ошибка создания записи в таблице fips_contactaddress для NSER = {}: {}".format(record_dictionary['retro_number'], error))


@time_test
def create_rutrademarkrepresentationfile(CONNECTION, CURSOR, root_object_uuid, nested_storage_obj, rutmk_uid_value, image_path, image_name, record_dictionary, received_date, image_mode): 
    # создание файла где будет хранится информация о товаре
    source_image_path = image_path
    try:
        image = Image.open(image_path)
        height, width = image.size
        represent_uid_value = uuid.uuid1()
        image_uid = uuid.uuid1()
        if image_mode == 'TIFF':
            extension = 'TIF'
            image_type = 'TIFF'
        elif image_mode == 'JPEG':
            converted = False
            extension = 'JPG'
            image_type = 'JPEG'
            try:
                image = Image.open(image_path).convert('RGB')
                image_path = '.'.join([image_path.split(".")[0], extension])
                image.save(image_path, 'JPEG', quality=80)
                converted = True
            except Exception as error:
                logging.error("Не удалось конвертировать изображение {} к ОХ {}: {}".format(image, root_object_uuid, error))

        if image_mode == 'JPEG' and converted == False:
            logging.info("Продолжается миграция без сохранения изображения в формате JPEG и записи в fips_rutrademarkrepresentationfile")    
        elif image_mode == 'TIFF' or (image_mode == 'JPEG' and converted == True):
            new_file_name = '{}_1_{}.{}'.format(image_uid.hex, image_name, extension)
            folders = [
                '{}'.format(received_date.strftime("%Y")),
                '{}'.format(received_date.strftime("%m")),
                '{}'.format(received_date.strftime("%d")),
                '{}'.format(root_object_uuid.hex),
                '{}'.format(nested_storage_obj.hex),
                'TRADEMARK_IMAGE'
            ]
            path = '{}\\'.format(DESTINATION_DIRECTORY) + "\\".join(folders)
            
            os.makedirs(path, exist_ok=True)
            target = os.path.join(path, new_file_name)
            shutil.copy2(source_image_path, target)

            final_path = NFS + '/'.join(folders) + '/'
            logging.info('Файл {}{} создан'.format(final_path, new_file_name))
            success = True

    except UnidentifiedImageError as e:
        logging.error("Ошибка миграции изображения {}: {}".format(image_name, e))
        success = False

    except Exception as ex:
        exc_type, _, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(ERROR_STRING.format(exc_type, file_name, exc_tb.tb_lineno, ex))
    
    if success == True:

        values_list = [
            str(represent_uid_value),
            str(rutmk_uid_value),
            final_path + new_file_name,
            new_file_name,
            image_type,
            str(image_uid),
            0,
            height,
            width,
            '',
            '',
            '',
            record_dictionary['representation_sign'],
            '',
            ''
        ]
        formatted_values = []
        for val in values_list:
            if type(val) == None:
                formatted_values.append('NULL')
            elif type(val) == datetime.datetime:
                val = val.strftime("%Y.%m.%d %H:%M:%S")
                formatted_values.append(val)
            elif type(val) == datetime.date:
                val = val.strftime("%Y.%m.%d")
                formatted_values.append(val)
            elif type(val) == int:
                val = str(val)
                formatted_values.append(val)
            elif type(val) == uuid:
                val = str(val)
                formatted_values.append(val)
            elif val == '':
                formatted_values.append('NULL')
            else:
                formatted_values.append(val)
        values = ', '.join(["'" + str(i) + "'" if i else 'NULL' for i in formatted_values])
        values = values.replace('\'NULL\'', 'NULL')
        print(values)
        
        ready_query = 'INSERT INTO "fips_rutrademarkrepresentationfile" (represent_uid, rutmk_uid, file_link, file_name, file_type, "content", "order", height, width, "length", description_text, representation_category, representation_sign, representation_colour, image_classification) VALUES({})'.format(values)
        print(ready_query)
        try:
            CURSOR.execute(ready_query)
            CONNECTION.commit()
            logging.info("Создана запись в таблице fips_rutrademarkrepresentationfile для NSER = {}".format(record_dictionary['retro_number']))
        except Exception as error:
            logging.error("Ошибка создания записи в таблице fips_rutrademarkrepresentationfile для NSER = {}: {}".format(record_dictionary['retro_number'], error))


@time_test
def get_nsers(connection, cursor):
    """Получает все NSER из базы данных в таблице fips_rutrademark"""
    nser_query = 'SELECT retro_number FROM fips_rutrademark ORDER BY retro_number;'
    cursor.execute(nser_query)
    nser_list = cursor.fetchall()
    nsers = []
    for nser in nser_list:
        nsers.append(nser[0])
    return nsers


@time_test
def get_rutmk_uid_stor_obj(connection, cursor, nser):
    """Получает значения rutmk_uid и object_uid, связанные с указанным NSER"""
    rutmkuid_query = 'select rutmk_uid, object_uid from fips_rutrademark where retro_number = {};'.format(nser)
    cursor.execute(rutmkuid_query)
    value = cursor.fetchone()
    rutmkuid_value = value[0]
    object_uid = value[1]
    return rutmkuid_value, object_uid


@time_test
def get_image_links(connection, cursor, rutmk_uid):
    """Получает информацию об изображения, относвящихся к заявке на ТЗ, согласно указанному rutmk_uid"""
    link_query = "select file_link from fips_rutrademarkrepresentationfile where rutmk_uid = '{}'".format(rutmk_uid)
    try:
        cursor.execute(link_query)
        values = cursor.fetchall()
        links = []
        for value in values:
            links.append(value[0])
        return links
    except Exception as error:
        logging.info("Для указанного rutmk_uid {} ссылки на изображения не найдены".format(rutmk_uid))


@time_test
def get_holder_uids(connection, cursor, rutmk_uid):
    """Получает идентификатор записи о заявителе"""
    holder_query = """SELECT fips_contact.contact_uid as holder_contact_uid, fips_contactaddress.address_uid as holder_contactaddress_uid
        FROM fips_rutrademark
        INNER JOIN fips_rutmkholder ON fips_rutrademark.rutmk_uid = fips_rutmkholder.rutmk_uid
        INNER JOIN fips_contact ON fips_rutmkholder.contact_uid = fips_contact.contact_uid
        INNER JOIN fips_contactaddress on fips_contact.contact_uid = fips_contactaddress.contact_uid
        WHERE fips_rutrademark.rutmk_uid = '{}';
    """.format(rutmk_uid)
    try:
        cursor.execute(holder_query)
        data = cursor.fetchone()
        contactuid = data[0]
        contactaddruid = data[1]
        return contactuid, contactaddruid
    except Exception as error:
        logging.error("Ошибка получения идентификатора записи о заявителе для rutmk_uid {}: {}".format(rutmk_uid, error))


@time_test
def get_representative_uids(connection, cursor, rutmk_uid):
    """Получает идентификатор записи о представителе"""
    representative_query = """SELECT fips_contact.contact_uid as repres_contact_uid, fips_contactaddress.address_uid as repres_contactaddress_uid
        FROM fips_rutrademark
        INNER JOIN fips_rutmkrepresentative ON fips_rutrademark.rutmk_uid = fips_rutmkrepresentative.rutmk_uid
        INNER JOIN fips_contact ON fips_rutmkrepresentative.contact_uid = fips_contact.contact_uid
        INNER JOIN fips_contactaddress on fips_contact.contact_uid = fips_contactaddress.contact_uid
        WHERE fips_rutrademark.rutmk_uid = '{}';
    """.format(rutmk_uid)
    try:
        cursor.execute(representative_query)
        data = cursor.fetchone()
        contactuid = data[0]
        contactaddruid = data[1]
        return contactuid, contactaddruid
    except Exception as error:
        logging.error("Ошибка получения идентификатора записи о представителе для rutmk_uid {}: {}".format(rutmk_uid, error))


@time_test
def get_corresp_addr_uid(connection, cursor, rutmk_uid):
    """Получает идентификтор записи об адресе для переписки"""
    corresp_addr_query = """SELECT fips_correspondenceaddress.address_uid as corresp_address_uid
        FROM fips_rutrademark
        INNER JOIN fips_rutmkcorrespondenceaddress ON fips_rutrademark.rutmk_uid = fips_rutmkcorrespondenceaddress.rutmk_uid
        INNER JOIN fips_correspondenceaddress ON fips_rutmkcorrespondenceaddress.address_uid = fips_correspondenceaddress.address_uid
        WHERE fips_rutrademark.rutmk_uid = '{}';
    """.format(rutmk_uid)
    try:
        cursor.execute(corresp_addr_query)
        data = cursor.fetchone()
        return data[0]
    except Exception as error:
        logging.error("Ошибка получения идентификатора записи об адресе для переписки для rutmk_uid {}: {}".format(rutmk_uid, error))


@time_test
def delete_record(connection, cursor, field_name, table_name, record_uid):
    """Удаляет запись из указанной таблицы по идентификатору записи"""
    try:
        delete_query = "DELETE FROM {} WHERE {} = '{}'".format(table_name, field_name, record_uid)
        cursor.execute(delete_query)
        connection.commit()
        logging.info('Запись в таблице {} удалена'.format(table_name))
    except Exception as error:
        logging.error("Ошибка при удалении записи {} из таблицы {}: {}".format(record_uid, table_name, error))


@time_test
def delete_storage_object(connection, cursor, storage_object_uid):
    """Удаляет запись об объекте хранения согласно указанному идентификатору"""
    delete_obj_query = """DELETE FROM "Objects" WHERE "Number" = '{}' or "ParentNumber" = '{}'""".format(storage_object_uid, storage_object_uid)
    try:
        cursor.execute(delete_obj_query)
        connection.commit()
        logging.info('Запись об объекте хранения {} и связанных с ним вложенными объектами хранения удалена'.format(storage_object_uid))
    except Exception as error:
        logging.error('Запись об объекте хранения {} не найдена: {}'.format(storage_object_uid, error))




def get_records_by_nser(connection, cursor, existing_nser_list):
    """По существующим в ЕХД NSER'ам получаем данные о rutmk_uid каждой записи"""
    get_nser_records_query = 'SELECT retro_number, rutmk_uid FROM fips_rutrademark WHERE retro_number IN existing_nsers'
    cursor.execute(get_nser_records_query)
    data = cursor.fetchall()
    data_dict = {}
    for el in data:
        data_dict[el[0]] = el[1]
    return data_dict

def get_related_record(connection, cursor, table_name, rutmk_uid):
    query = 'SELECT * FROM {} WHERE rutmk_uid = {}'.format(table_name, rutmk_uid)
    cursor.execute(query)
    record = cursor.fetchone()
    return record


def migrate(): #ПРОВЕРЬ НОМЕР ВЫГРУЗКИ!
    logging.info("Начинается создание массива (словаря) из записей АСТЗ РФ для миграции")
    big_dictionary = collect_data("{}ready00000001".format(IMPORT_DIRECTORY))
    logging.info("Словарь создан, начинается миграция")

    nsers = get_nsers(CONNECTION, CURSOR)

    count = 0
    count1 = 0
    for k, v in big_dictionary.items():

        record_dictionary = get_record_dict(big_dictionary[k])
        print(record_dictionary)

        # получение данных для RUTmkGoodsServices
        goods_info = record_dictionary['goods_services']
        if goods_info is not None and goods_info != 'None':
            numbers = re.findall('[0-9]+', goods_info)
            for num in numbers:
                if len(num) < 2:
                    numbers.remove(num)
            if len(numbers) > 1:
                goods_classes_list = numbers  
            elif len(numbers) == 1:
                goods_class = numbers[0]  

            # убираем номера классов для definition в RUTmkGoodsServices
            str_wout_digits = []
            for i in goods_info:
                if not i.isdigit():
                    str_wout_digits.append(i)
            if len(numbers) == 1:
                goods_definition_draft = ''.join(str_wout_digits)
                goods_definition = delete_commas(
                    goods_definition_draft)  
            elif len(numbers) > 1:
                goods_definition_list = []  
                for i in range(len(goods_classes_list)):
                    goods_definition_list.append('<см. общее описание классов МКТУ>')
        else:
            goods_info = ''

        if record_dictionary['retro_number'] in nsers:
            nser_index = nsers.index(record_dictionary['retro_number'])
            rutmkuid_value, objectuid_value = get_rutmk_uid_stor_obj(CONNECTION, CURSOR, record_dictionary['retro_number'])
            print(rutmkuid_value, objectuid_value)
            image_links = get_image_links(CONNECTION, CURSOR, rutmkuid_value)
            with open('image_links.txt', 'a') as f:
                for item in image_links:
                    f.write("%s\n" % item)
            holder_contact_uid, holder_contactaddr_uid = get_holder_uids(CONNECTION, CURSOR, rutmkuid_value)
            repres_contact_uid, repres_contactaddr_uid = get_representative_uids(CONNECTION, CURSOR, rutmkuid_value)
            corresp_addr_uid = get_corresp_addr_uid(CONNECTION, CURSOR, rutmkuid_value)

            # удаляем записи с устаревшей информацией:
            delete_record(CONNECTION, CURSOR, "address_uid", "fips_contactaddress", holder_contactaddr_uid)
            delete_record(CONNECTION, CURSOR, "rutmk_uid", "fips_rutmkholder", rutmkuid_value)
            delete_record(CONNECTION, CURSOR, "rutmk_uid", "fips_rutmkapplicant", rutmkuid_value)
            delete_record(CONNECTION, CURSOR, "contact_uid", "fips_contact", holder_contact_uid)
            delete_record(CONNECTION, CURSOR, "address_uid", "fips_contactaddress", repres_contactaddr_uid)
            delete_record(CONNECTION, CURSOR, "rutmk_uid", "fips_rutmkrepresentative", rutmkuid_value)
            delete_record(CONNECTION, CURSOR, "contact_uid", "fips_contact", repres_contact_uid)
            delete_record(CONNECTION, CURSOR, "rutmk_uid", "fips_rutrademarkrepresentationfile", rutmkuid_value)
            delete_record(CONNECTION, CURSOR, "rutmk_uid", "fips_rutmkcorrespondenceaddress", rutmkuid_value)
            delete_record(CONNECTION, CURSOR, "address_uid", "fips_correspondenceaddress", corresp_addr_uid)
            delete_record(CONNECTION, CURSOR, "rutmk_uid", "fips_rutmkdisclaimer", rutmkuid_value)
            delete_record(CONNECTION, CURSOR, "rutmk_uid", "fips_rutmkgoodsservices", rutmkuid_value)
            delete_record(CONNECTION, CURSOR, "rutmk_uid", "fips_rutmkpriority", rutmkuid_value)
            delete_record(CONNECTION, CURSOR, "rutmk_uid", "fips_rutrademark", rutmkuid_value)
            delete_storage_object(CONNECTION, CURSOR, objectuid_value)
            nsers.pop(nser_index)
            logging.info("Дубликат удален")

        received_date = record_dictionary['appl_date'] if record_dictionary['appl_date'] is not None and record_dictionary['appl_date'] != 'None' else datetime.datetime.now()
        root_object_uuid = get_storage_obj(CONNECTION, CURSOR, received_date)
        print('ОХ:', root_object_uuid)
        rutmk_uid_value = uuid.uuid1()
        print('rutmk_uid:', rutmk_uid_value)
        update_time = datetime.datetime.now()
        create_rutrademark(CONNECTION, CURSOR, record_dictionary, root_object_uuid, rutmk_uid_value, update_time, NULL_VALUE)

        # priorities
        if record_dictionary['conven_priority_date'] is None and record_dictionary['exhib_priority_date'] is None and record_dictionary['first_priority_date']:
            priority_type = PRIORITY_DICT['UNKNOWN']  
            priority_date = datetime.date(1, 1, 1)  
            priority_appl_number = ''  
            priority_appl_date = ''  
            create_rutmkpriority(CONNECTION, CURSOR, record_dictionary, rutmk_uid_value, priority_type, priority_date,
                                 priority_appl_number, priority_appl_date)
        else:
            if record_dictionary['conven_priority_date'] is not None and record_dictionary['conven_priority_date'] != 'None':
                priority_type = PRIORITY_DICT['DAPK'] 
                priority_date = record_dictionary['conven_priority_date'] 
                priority_appl_number = '' 
                priority_appl_date = '' 
                create_rutmkpriority(CONNECTION, CURSOR, record_dictionary, rutmk_uid_value, priority_type, priority_date, priority_appl_number, priority_appl_date)
            if record_dictionary['exhib_priority_date'] is not None and record_dictionary['exhib_priority_date'] != 'None':
                priority_type = PRIORITY_DICT['DAPV']  
                priority_date = record_dictionary['exhib_priority_date']  
                priority_appl_number = '' 
                priority_appl_date = '' 
                create_rutmkpriority(CONNECTION, CURSOR, record_dictionary, rutmk_uid_value, priority_type, priority_date,
                                     priority_appl_number, priority_appl_date)
            if record_dictionary['first_priority_date'] is not None and record_dictionary['first_priority_date'] != 'None':
                priority_type = PRIORITY_DICT['DFAP']  
                priority_date = record_dictionary['first_priority_date']  
                priority_appl_number = ''  
                priority_appl_date = ''  
                create_rutmkpriority(CONNECTION, CURSOR, record_dictionary, rutmk_uid_value, priority_type, priority_date,
                                     priority_appl_number, priority_appl_date)

        address_uid_value = uuid.uuid1()
        create_correspondenceaddress(CONNECTION, CURSOR, record_dictionary, address_uid_value, update_time)
        create_rutmkcorrespondenceaddress(CONNECTION, CURSOR, record_dictionary, rutmk_uid_value, address_uid_value)
        if record_dictionary['goods_services'] is not None and record_dictionary['goods_services'] != 'None':
            if len(numbers) == 1:
                g_class, definition = goods_class, goods_definition
                create_rutmkgoodsservices(CONNECTION, CURSOR, record_dictionary, rutmk_uid_value, g_class,
                                          definition)
            elif len(numbers) > 1:
                goods_data = list(zip(goods_classes_list, goods_definition_list))
                for el in goods_data:
                    g_class, definition = el[0], el[1]
                    create_rutmkgoodsservices(CONNECTION, CURSOR, record_dictionary, rutmk_uid_value, g_class, definition)
        if record_dictionary['name_holder_applicant'] is not None and record_dictionary['name_holder_applicant'] != '':
            holder_applicant_contact_uid_value = uuid.uuid1()
            create_contact(CONNECTION, CURSOR, record_dictionary, update_time, holder_applicant_contact_uid_value, mode='holder_or_applicant')
            create_rutmkapplicant(CONNECTION, CURSOR, holder_applicant_contact_uid_value, rutmk_uid_value, record_dictionary)
            create_rutmkholder(CONNECTION, CURSOR, holder_applicant_contact_uid_value, rutmk_uid_value, record_dictionary)
            create_contactaddress(CONNECTION, CURSOR, holder_applicant_contact_uid_value, record_dictionary, update_time, mode='holder_or_applicant')
        if record_dictionary['name_representative'] is not None and record_dictionary['name_representative'] != '':
            representative_contact_uid_value = uuid.uuid1()
            create_contact(CONNECTION, CURSOR, record_dictionary, update_time, representative_contact_uid_value, mode='representative')
            create_rutmkrepresentative(CONNECTION, CURSOR, representative_contact_uid_value, rutmk_uid_value, record_dictionary)
            create_contactaddress(CONNECTION, CURSOR, representative_contact_uid_value, record_dictionary, update_time, mode='representative')
        if record_dictionary.get('image_path') is not None:
            if record_dictionary['image_path'] is not None:
                image_path = record_dictionary['image_path']
                image_name = record_dictionary['image_name']
                parent_number = root_object_uuid
                nested_storage_obj = get_storage_obj(CONNECTION, CURSOR, received_date, parent_number)
                image_mode = 'TIFF'
                create_rutrademarkrepresentationfile(CONNECTION, CURSOR, root_object_uuid, nested_storage_obj, rutmk_uid_value, image_path, image_name, record_dictionary, received_date, image_mode)
                image_mode = 'JPEG'
                create_rutrademarkrepresentationfile(CONNECTION, CURSOR, root_object_uuid, nested_storage_obj, rutmk_uid_value, image_path, image_name, record_dictionary, received_date, image_mode)
        count += 1
        if count >= 10000:
            count1 += count
            logging.info('{} записей'.format(count1))
            count = 0


if __name__ == '__main__':
    migrate()