import os
import time
from http import HTTPStatus
from typing import List

import dask.dataframe as dd
import requests

import constants
import headers
from logger import app_logger


class PriceDataProcessor:
    CHUNK_SIZE = 1024 * 16
    REQUIRED_CSV_FORMAT_COLUMNS_COUNT = 16
    REFERER = 'https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads'

    def __init__(self):
        self.errors = False
        self.problem_urls = []

    @staticmethod
    def del_file(fpath):
        try:
            os.remove(fpath)
        except Exception as e:
            app_logger.warning("Unable to remove file %s, exception occurred: %s" % (fpath, str(e)))

    @staticmethod
    def clear_dir(dir):
        for f in os.listdir(dir):
            PriceDataProcessor.del_file(os.path.join(dir, f))

    @staticmethod
    def get_file_name(url) -> str:
        parts = url.rsplit('/', 1)
        return parts[-1]

    def get_errors(self) -> bool:
        return self.errors

    def get_problem_urls(self) -> List[str]:
        return self.problem_urls

    def fetch_data(self) -> bool:
        self.errors = False
        self.problem_urls = []

        self.clear_dir(constants.DATA_DIR)
        self.clear_dir(constants.PROCESSED_DATA_DIR)

        app_logger.info("Fetching started")

        if not constants.DOWNLOAD_LINKS:
            app_logger.warning("no links to download")
            self.errors = True
            return False

        start = time.time()

        for url in constants.DOWNLOAD_LINKS:
            app_logger.info("downloading %s..." % url)

            name = self.get_file_name(url)

            h = headers.get_random_headers()
            h['Referer'] = self.REFERER

            try:
                resp = requests.get(url=url, stream=True, headers=h)
                if resp.status_code != HTTPStatus.OK:
                    app_logger("Request error: bad response code " + str(resp.status_code))
                    self.problem_urls.append(url)
                    self.errors = True
                    continue

                app_logger.info("saving %s..." % url)

                with open(constants.DATA_DIR / name, 'wb') as f:
                    for chunk in resp.iter_content(chunk_size=self.CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)

                app_logger.info("saved")

            except requests.exceptions.RequestException as e:
                app_logger.error("Request error: " + str(e))
                self.problem_urls.append(url)
                self.errors = True
            except Exception as e:
                app_logger.error("General error: " + str(e))
                self.problem_urls.append(url)
                self.errors = True

                # remove file data leftovers in case of errors
                # (it may be corrupted, incomplete, etc)
                self.del_file(constants.DATA_DIR / name)

        app_logger.info("Fetching data completed in %s seconds" % str(time.time() - start))

        # check if at least some urls have been downloaded without problem
        return not self.errors or (len(self.problem_urls) < len(constants.DOWNLOAD_LINKS))

    @staticmethod
    def preprocess_data() -> bool:
        app_logger.info("Preparing data started...")

        start = time.time()

        series: List[dd.Series] = []
        for f in os.listdir(constants.DATA_DIR):
            if not f.endswith(".csv"):
                app_logger.warning("non-CSV file found in DATA_DIR: %s" % f)
                continue

            app_logger.info("Processing %s" % f)
            try:
                if len(series) < 2:
                    df = dd.read_csv(constants.DATA_DIR / f, header=None)

                    if len(df.columns) != PriceDataProcessor.REQUIRED_CSV_FORMAT_COLUMNS_COUNT:
                        app_logger.error("File %s has insufficient amount of columns: required %d, found %d"
                                         % (f, PriceDataProcessor.REQUIRED_CSV_FORMAT_COLUMNS_COUNT, len(df.columns)))
                        continue

                    # we are interested in the 4th column's values
                    fourth_col: dd.Series = df.iloc[:, 3]
                    unique_vals_series = fourth_col.drop_duplicates()
                    series.append(unique_vals_series)
                
                if len(series) == 2:
                    # merge two Series into one and remove duplicates
                    s = dd.concat(series).drop_duplicates()

                    # keep the result Series in the first list's element
                    del series[-1]
                    series[0] = s

            except Exception as e:
                app_logger.error("Processing file %f had errors: " + str(e))

            app_logger.info("Processing %s done" % f)

        if series:
            s: dd.Series = series[0]
            s.to_csv(constants.PROCESSED_DATA_DIR / "single.csv", single_file=True, index=False, header=False)
        else:
            app_logger.error("Prepare data: could not generate the result CSV file")

        app_logger.info("Preparing data completed in %s seconds" % str(time.time() - start))
        return bool(series)
