import os
import hashlib
import tempfile
import pickle
from common_constants import constants
from datetime import date
ENVI = constants.EnviVar(
    main_dir="/home/eugene/Yandex.Disk/localsource/google_analytics/",
    cred_dir="/home/eugene/Yandex.Disk/localsource/credentials/"
)
logger = constants.logging.getLogger(__name__)


class MemoryDiscoveryCache:
    """
    https://github.com/googleapis/google-api-python-client/tree/master/googleapiclient/discovery_cache
    https://github.com/googleapis/google-api-python-client/issues/325
    Based on Schweigi's solution on 22 Jan 2017
    """
    _CACHE = {}

    @staticmethod
    def get(url):
        return MemoryDiscoveryCache._CACHE.get(url)

    @staticmethod
    def set(url, content):
        MemoryDiscoveryCache._CACHE[url] = content


class TmpFileDiscoveryCache:
    """
    https://github.com/googleapis/google-api-python-client/tree/master/googleapiclient/discovery_cache
    https://github.com/googleapis/google-api-python-client/issues/325
    Based on Schweigi's and Chronial solutions on 7 Sep 2018
    """
    @staticmethod
    def filename(url):
        return os.path.join(
            tempfile.gettempdir(),
            'google_api_discovery_' + hashlib.md5(url.encode()).hexdigest())

    def get(self, url):
        try:
            with open(self.filename(url), 'rb') as f:
                return f.read().decode()
        except FileNotFoundError:
            return None

    def set(self, url, content):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content.encode())
            f.flush()
            os.fsync(f)
        os.rename(f.name, self.filename(url))


class DumpFileDiscoveryCache:
    """
    https://github.com/googleapis/google-api-python-client/tree/master/googleapiclient/discovery_cache
    https://github.com/googleapis/google-api-python-client/issues/325
    Based on Schweigi's and Chronial solutions on 7 Sep 2018
    """
    @staticmethod
    def filename(url):
        return f'{ENVI["MAIN_PYSEA_DIR"]}alldata/cache/' \
               f'google_api_discovery_{date.today()}_{hashlib.md5(url.encode()).hexdigest()}.pickle'

    def get(self, url):
        try:
            with open(self.filename(url), 'rb') as f:
                return pickle.load(f)
        except FileNotFoundError:
            return None

    def set(self, url, content):
        logger.debug(f"DumpFileDiscoveryCache SET: {content}")
        with open(self.filename(url), "wb") as f:
            pickle.dump(content, f, pickle.HIGHEST_PROTOCOL)
