from __future__ import annotations

from datetime import date, timedelta
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
from common_constants import constants
from google_analytics import analyticscache
from collections import deque
import pickle

from time import sleep
from urllib3.exceptions import ProtocolError
from http.client import RemoteDisconnected
from googleapiclient.errors import HttpError
from socket import timeout


ENVI = constants.EnviVar(
    main_dir="/home/eugene/Yandex.Disk/localsource/google_analytics/",
    cred_dir="/home/eugene/Yandex.Disk/localsource/credentials/"
)
logger = constants.logging.getLogger(__name__)


class GoogleAnalyticsError(constants.PySeaError): pass
class LimitOfRetryError(GoogleAnalyticsError): pass


def updatable_dump_to(prefix):
    """
    Декоратор для частичного кеширования.
    Например, запрос данных из Google Analytics,
    только для недостающих дат (или словарей) => на самом деле определяется функцией _set_cache_data

    Применим к методам класса, в котором объявлены:
    self.directory - ссылка на каталог
    self.dump_file_prefix - файловый префикс
    self.cache - True - кеширование требуется / False
    метод _set_cache_data
    На вход принимает префикс, который идентифицирует декорируемую функцию

    Кеш хранится в сериализованных файлах с помощью pickle

    :param prefix: идентифицирует декорируемую кешируемую функцию
    :return:
    """
    def deco_dump(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(self, *argp, **argn):  # конструируемая функция
            file_out = "{}/{}_{}_data.pickle".format(self.directory, self.dump_file_prefix, prefix).replace("//", "/")
            read_data = None

            if self.cache:  # если кеширование требуется
                try:  # пробуем прочитать из файла
                    with open(file_out, "rb") as file:
                        read_data = pickle.load(file)
                except Exception as msg:
                    print(msg)
                    pass

            self._set_cache_data(read_data)
            read_data = f(self, *argp, **argn)
            with open(file_out, "wb") as file:  # записываем результат в файл
                pickle.dump(read_data, file, pickle.HIGHEST_PROTOCOL)
            return read_data
        return constructed_function
    return deco_dump


def dump_to(prefix, d=False):  # конструктор декоратора (n залипает в замыкании)
    """
    Декоратор для кеширования возврата функции.
    Применим к методам класса, в котором объявлены:
    self.directory - ссылка на каталог
    self.dump_file_prefix - файловый префикс
    self.cache - True - кеширование требуется / False
    На вход принимает префикс, который идентифицирует декорируемую функцию

    Кеш хранится в сериализованных файлах с помощью pickle

    :param prefix: идентифицирует декорируемую кешируемую функцию
    :param d: явно указанная дата в self.current_date или False для сегодняшней даты (для формирования имени файла)
    :return:
    """
    def deco_dump(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(self, *argp, **argn):  # конструируемая функция
            if 'dump_parts_flag' in self.__dict__:
                dump_file_prefix = f"{self.dump_file_prefix}_p{self.dump_parts_flag['part_num']}"
            else:
                dump_file_prefix = self.dump_file_prefix

            if not d:
                file_out = "{}/{}_{}_{}.pickle".format(self.directory, dump_file_prefix, prefix,
                                                     date.today()).replace("//", "/")
            else:
                file_out = "{}/{}_{}_{}.pickle".format(self.directory, dump_file_prefix, prefix,
                                                     self.current_date).replace("//", "/")
            read_data = ""

            if self.cache:  # если кеширование требуется
                try:  # пробуем прочитать из файла
                    with open(file_out, "rb") as file:
                        read_data = pickle.load(file)
                except Exception as err:
                    logger.debug(f"{err}\n Cache file {file_out} is empty, getting fresh...")

            if not read_data:  # если не получилось то получаем данные прямым вызовом функции
                read_data = f(self, *argp, **argn)
                if 'dump_parts_flag' in self.__dict__:
                    self.dump_parts_flag['len'] = len(read_data)

                with open(file_out, "wb") as file:  # записываем результат в файл
                    if 'dump_parts_flag' in self.__dict__:
                        pickle.dump(read_data[-self.dump_parts_flag['len']:], file, pickle.HIGHEST_PROTOCOL)
                    else:
                        pickle.dump(read_data, file, pickle.HIGHEST_PROTOCOL)
            return read_data
        return constructed_function
    return deco_dump


def connection_attempts(n=12, t=10):  # конструктор декоратора (N,T залипает в замыкании)
    """
    Декоратор задает n попыток для соединения с сервером в случае ряда исключений
    с задержкой t*2^i секунд

    :param n: количество попыток соединения с сервером [1, 15]
    :param t: количество секунд задержки на первой попытке попытке (на i'ом шаге t*2^i)
    :return:
    """
    def deco_connect(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(*argp, **argn):  # конструируемая функция
            retry_flag, pause_seconds = n, t
            try_number = 0

            if retry_flag < 0 or retry_flag > 15:
                retry_flag = 8
            if pause_seconds < 1 or pause_seconds > 30:
                pause_seconds = 10

            while True:
                try:
                    result = f(*argp, **argn)
                    # Обработка ошибки, если не удалось соединиться с сервером
                except (ConnectionError,
                        ProtocolError, RemoteDisconnected,
                        HttpError, timeout) as err:
                    logger.error(f"Ошибка соединения с сервером {err}. Осталось попыток {retry_flag - try_number}")
                    if try_number >= retry_flag:
                        raise LimitOfRetryError
                    sleep(pause_seconds * 2 ** try_number)
                    try_number += 1
                    continue
                else:
                    return result

            return None
        return constructed_function
    return deco_connect


def handle_v3_errors(f):
    def constructed_function(self, *argp, **argn):
        try:
            result = f(self, *argp, **argn)
        except TypeError as error:
            # Handle errors in constructing a query.
            logger.exception(f"Ошибка при создании запроса Analytics v3: {error}")
            raise TypeError
        else:
            return result

        # return None
    return constructed_function


def limit_by(page_size=1000, rows_or_full="rows"):  # конструктор декоратора (L залипает в замыкании)
    """
    Декоратор для использования постраничной выборки
    https://developers.google.com/analytics/devguides/reporting/core/v4/basics#pagination
    Декоратор применим, только для запросов с одним отчетом (reportRequests)

    :param page_size: не более 10 000 объектов за один запрос.
    :return: возвращает только данные массива rows
    """
    def deco_limit(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(self, *argp, **argn):  # конструируемая функция
            result = []

            self.pageSize = abs(page_size) if abs(page_size) <= 10000 else 10000

            data = f(self, *argp, **argn)
            if rows_or_full == "rows":
                result.extend(data['reports'][0]['data'].get("rows", []))
            else:
                result.append(data)

            while data['reports'][0].get('nextPageToken', False):
                self.pageToken = data['reports'][0]['nextPageToken']
                data = f(self, *argp, **argn)
                if rows_or_full == "rows":
                    result.extend(data['reports'][0]['data'].get("rows", []))
                else:
                    result.append(data)

            self.pageToken = 0  # не забываем вернуть пагенатор в исходное состояние для следующих вызовов
            return result
        return constructed_function
    return deco_limit


class DateDeque(deque):
    def __contains__(self, item):
        for i in self:
            if i[0] == item:
                return True
        else:
            return False

    def get_by_date(self, item):
        for i in self:
            if i[0] == item:
                return i
        return None

    def sort_by_date(self):
        """
        Обычно даты отчета поступают последовательно по возрастанию,
        но если вдруг нет уверенности, то можно воспользоваться этим методом для сортировки
        :return:
        """
        items = sorted(self, key=lambda x: x[0], reverse=False)
        self.clear()
        self.extend(items)

    def clear_dates_before(self, d):
        # чистим устаревшие даны из кэша
        while len(self):
            if self[0][0] < d:
                self.popleft()
            else:
                break


class GoogleAnalyticsBase:
    """
    https://developers.google.com/analytics/devguides/reporting/core/v4/basics#segments
    https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet
    """
    def __init__(self, directory: str = "./",
                 dump_file_prefix: str = "fooooo",
                 cache: bool = True) -> None:

        self.scopes = ['https://www.googleapis.com/auth/analytics.readonly']
        self.view_id = ENVI['PYSEA_ANALYTICS_VIEW_ID']
        self.analytics = None
        self.date_ranges = [{'startDate': 'yesterday', 'endDate': 'yesterday'}]
        self.begin_date = date.today() - timedelta(1)
        self.end_date = date.today() - timedelta(1)

        # переменные настраивающие кеширование запросов к API
        self.directory = directory
        self.dump_file_prefix = dump_file_prefix
        self.cache = cache

        # https://developers.google.com/analytics/devguides/reporting/core/v4/resource-based-quota
        self.use_resource_quotas = False

        # переменные устанавливают постраничные запросы к API
        self.pageSize = 25
        self.pageToken = 0
        self.cache = cache

        # дека для хранения отчетов по дням
        self.data = DateDeque()

        # множество целей и конверсий
        self.collect_only_golden_data = False

        # альтернативный период для Golden данных
        self.golden_begin_date = self.begin_date
        self.golden_end_date = self.begin_date

    def _initialize_analytics_service(self, version: str = "v4") -> discovery.Resource:
        """
        Initializes an Analytics Reporting API V4 service object.
        Returns: An authorized Analytics Reporting API V4 service object.
        """
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            f'{ENVI["CREDENTIALS_DIR"]}EK-GA-project-2599388e697a.json', self.scopes)

        # Build the service object.
        if version == "v3":
            analytics_service = discovery.build("analytics", "v3", credentials=credentials,
                                                cache=analyticscache.DumpFileDiscoveryCache())
        elif version == "v4":
            analytics_service = discovery.build('analyticsreporting', 'v4', credentials=credentials,
                                                cache=analyticscache.DumpFileDiscoveryCache())
        else:
            analytics_service = discovery.build('analyticsreporting', 'v4', credentials=credentials,
                                                cache=analyticscache.DumpFileDiscoveryCache())

        return analytics_service

    def use_app_view_id(self) -> None:
        self.view_id = ENVI['PYSEA_ANALYTICS_MOBILEVIEW_ID']

    def use_site_view_id(self) -> None:
        self.view_id = ENVI['PYSEA_ANALYTICS_VIEW_ID']

    def tune_for_site_view_id(self) -> None:
        # функция для переопределения
        pass

    def tune_for_app_view_id(self) -> None:
        # функция для переопределения
        pass

    def set_collect_only_golden_data(self, val: bool = True) -> None:
        self.collect_only_golden_data = val

    def cache_enabled(self) -> None:
        self.cache = True

    def cache_disabled(self) -> None:
        self.cache = False

    def resource_quotas_enable(self) -> GoogleAnalyticsBase:
        self.use_resource_quotas = True
        return self

    def resource_quotas_disable(self) -> GoogleAnalyticsBase:
        self.use_resource_quotas = False
        return self

    def set_data_range(self, begin: str, end: str = "") -> None:
        """
        Устанавливает период для запроса отчета Google Analytics
        Начальная и конечная даты должны быть заданы в формате ISO 8601 YYYY-MM-DD

        :param begin: YYYY-MM-DD
        :param end: YYYY-MM-DD
        :return:
        """
        if not end:
            end = begin

        self.begin_date = begin if type(begin) is date else date.fromisoformat(begin)
        self.end_date = end if type(end) is date else date.fromisoformat(end)
        self.date_ranges = [{'startDate': self.begin_date.isoformat(), 'endDate': self.end_date.isoformat()}]

    def _set_cache_data(self, cache_data: DateDeque) -> GoogleAnalyticsBase:
        if type(cache_data) is DateDeque:
            self.data = cache_data
        return self

    @staticmethod
    def print_response(response: dict):
        """
        Parses and prints the Analytics Reporting API V4 response.
        Args:
            response: An Analytics Reporting API V4 response.
        """
        for report in response.get('reports', []):
            column_header = report.get('columnHeader', {})
            dimension_headers = column_header.get('dimensions', [])
            metric_headers = column_header.get('metricHeader', {}).get('metricHeaderEntries', [])

            for j, row in enumerate(report.get('data', {}).get('rows', [])):

                dimensions = row.get('dimensions', [])
                date_range_values = row.get('metrics', [])

                for header, dimension in zip(dimension_headers, dimensions):
                    print(header + ': ' + dimension)

                for i, values in enumerate(date_range_values):
                    print('Date range: ' + str(i))
                    for metric_header, value in zip(metric_headers, values.get('values')):
                        print(metric_header.get('name') + ': ' + value)

    def batch_get_requests(self, requests: dict, golden_only: bool = False) -> dict:
        if self.analytics is None:
            self.analytics = self._initialize_analytics_service("v4")
        result = self.analytics.reports().batchGet(body=requests).execute()

        logger.info(f"Quotas after request {result['resourceQuotasRemaining']}")
        for num, i in enumerate(result['reports']):
            read_counts = i['data'].get('samplesReadCounts', False)
            space_sizes = i['data'].get('samplingSpaceSizes', False)
            data_golden = i['data'].get('isDataGolden', False)
            if read_counts:
                for j in range(len(read_counts)-1):
                    logger.warning(f"SAMPLING: Google Analytics\n "
                                   f"ответ с выборкой {read_counts[j]}/{space_sizes[j]} "
                                   f"= {read_counts[j] / space_sizes[j]}")
            if not data_golden:
                logger.warning("NOT GOLDEN: точно такой же запрос, сделанный позже, может вернуть новый результат")
                if golden_only:
                    logger.warning(f"Данная точка не будет учтена т.к. golden_only = {golden_only}")
                    result['reports'][num]['data'].pop("rows")

            sampling_levels = {i.get('samplingLevel', False) for i in requests["reportRequests"]}
            if len(sampling_levels) > 1 or sampling_levels.pop() is not False:
                logger.warning(f"установлен samplingLevel в запросе: {requests['reportRequests'][0]['samplingLevel']}")

        return result

    def __repr__(self) -> str:
        return f"{type(self)} ({self.begin_date.isoformat()} - {self.end_date.isoformat()})"


def example_batch_get_requests():
    directory = f"{ENVI['MAIN_PYSEA_DIR']}alldata/dump/"
    analytics = GoogleAnalyticsBase(
        directory=directory,
        dump_file_prefix="test_case",
        cache=False
    )

    analytics.set_data_range("2020-01-30", "2020-01-30")
    requests = {
        'reportRequests': [
            {
                'viewId': analytics.view_id,
                'dateRanges': analytics.date_ranges,
                'metrics': [{'expression': 'ga:sessions'}],
                'samplingLevel': "LARGE",
                "pageToken": str(analytics.pageToken),
                "pageSize": str(analytics.pageSize),
            }
        ],
        "useResourceQuotas": "true"
    }
    result = analytics.batch_get_requests(requests)
    print(result)


if __name__ == '__main__':
    example_batch_get_requests()
    print("QKRQ!")
