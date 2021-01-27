import random
import re
import time
from collections import namedtuple
from http import HTTPStatus
from typing import Optional, List

import bs4
import pandas as pd
import requests

import headers
from constants import RESULTS_DIR, PROCESSED_DATA_DIR
from logger import app_logger

ResultItem = namedtuple('ResultItem', ["address", "council_tax_band", "local_auth_ref_number"])
ResultSummary = namedtuple("ResultSummary", ["items", "pages_count"])


class PostCodesSearcher:
    SEARCH_INIT_URL = 'http://cti.voa.gov.uk/cti/InitS.asp?lcn=0&refresh={}'
    NEXT_RESULTS_URL = 'http://cti.voa.gov.uk/cti/RefSResp.asp?lcn=0'

    RX_PAGES_INFO_PATTERN = re.compile(r"^\r\n\s+Page\s(\d+)\s\r\n\s+of\s\r\n\s+(\d+)")

    def __init__(self):
        self.errors = False

    @staticmethod
    def gen_rand_number(ndigits: int):
        return ''.join(["{}".format(random.randint(0, 9)) for _ in range(0, ndigits)])

    @staticmethod
    def parse_info(text, scrape_pages_count: bool) -> Optional[ResultSummary]:

        bs = bs4.BeautifulSoup(text, features='html.parser')

        content = bs.find("div", {"id": "Content"})
        if not content:
            app_logger.error("Invalid response HTML format, 'Content' element not found")
            return None

        if not scrape_pages_count:
            pages_count = None
        else:
            pages_count = 1
            pagelist = content.find("div", {"class": "pagelist"})
            if pagelist:
                pages_info = pagelist.find("p", attrs={'class': None})
                if pages_info:
                    pages_val = pages_info.text.rstrip()
                    pattern_match = PostCodesSearcher.RX_PAGES_INFO_PATTERN.match(pages_val)
                    if pattern_match:
                        pages_count = int(pattern_match.group(2))

        search_results_tbl = content.find("table", {"title": "Search results"})
        if not search_results_tbl:
            app_logger.error("Invalid response HTML format, no 'Search results' table found")
            return None

        search_results_tbl_tbody = search_results_tbl.find("tbody")
        if not search_results_tbl_tbody:
            app_logger.error("Invalid response HTML format, no 'Search results' table's tbody found")
            return None

        result = []
        trs = search_results_tbl_tbody.find_all("tr")
        if trs:
            for tr in trs:
                tds = tr.find_all("td")
                if len(tds) != 4:
                    app_logger.error("Incorrect result table structure, should contain 4 columns, but %d found"
                                     % len(tds))
                    continue

                address_href = tds[0].find("a")
                if not address_href:
                    app_logger.error("Incorrect result table structure, no 'address' element found")
                    continue

                address_val = address_href.text.strip()
                council_tax_band_val = tds[1].text.strip()
                local_auth_ref_number_val = tds[3].text.strip()

                result.append(ResultItem(address=address_val,
                                         council_tax_band=council_tax_band_val,
                                         local_auth_ref_number=local_auth_ref_number_val))

        return ResultSummary(items=result, pages_count=pages_count)

    def query_impl(self, postcode, page) -> Optional[ResultSummary]:

        if page == 1:
            request_data = dict(btnPush=1,
                                txtRedirectTo='InitS.asp',
                                txtStartKey="0",
                                txtPageNum="0",
                                txtPageSize="",
                                intNumFound="",
                                txtPostCode=postcode)
            request_url = self.SEARCH_INIT_URL.format(PostCodesSearcher.gen_rand_number(12))
        else:
            request_data = dict(lstPageSize="20",
                                txtRefSPostCode=postcode,
                                txtStartKey=str((page - 1) * 20),
                                txtPageNum=str(page),
                                txtPageSize="20",
                                txtPostCode=postcode)
            request_url = self.NEXT_RESULTS_URL

        h = headers.get_random_headers()
        h['Referer'] = PostCodesSearcher.SEARCH_INIT_URL

        try:
            resp = requests.post(url=request_url,
                                 data=request_data,
                                 headers=h)
            if resp.status_code != HTTPStatus.OK:
                app_logger.error("Request error: bad response code " + str(resp.status_code))
                self.errors = True
                return None

            return self.parse_info(text=resp.text, scrape_pages_count=page == 1)

        except requests.exceptions.RequestException as e:

            app_logger.error("Request error: " + str(e))
            self.errors = True
            return None
        except Exception as e:

            app_logger.error("General error: " + str(e))
            self.errors = True
            return None

    def query(self, postcode) -> List[ResultItem]:
        summary = self.query_impl(postcode=postcode, page=1)

        if summary:
            if summary.pages_count > 1:
                # scrape additional pages if there's more than 1 page in results
                for p in range(2, summary.pages_count+1):
                    page_summary = self.query_impl(postcode=postcode, page=p)
                    if page_summary and page_summary.items:
                        summary.items.extend(page_summary.items)
            return summary.items
        return []

    def run(self):
        postcodes_file_path = PROCESSED_DATA_DIR / "single.csv"

        if not postcodes_file_path.exists():
            app_logger.error("No 'single.csv' file %s found. Exiting..." % postcodes_file_path)
            return

        # order of column names is important
        columns = ["Address", "Postcode", "Council Tax band", "Local authority reference number"]

        for chunk_df in pd.read_csv(postcodes_file_path, chunksize=100, header=None, usecols=[0]):
            for _, row in chunk_df.iterrows():

                postcode = row[0]
                result_file = RESULTS_DIR / "{}.csv".format(postcode.replace(" ", "_"))
                if result_file.exists():
                    app_logger.warning("Skipping result file %s, already exists" % result_file)
                    continue

                app_logger.info("Scraping %s postcode started" % postcode)
                items = self.query(postcode=postcode)

                result_list = [[result_item.address,
                                postcode,
                                result_item.council_tax_band,
                                result_item.local_auth_ref_number] for result_item in items] if items else []

                result_df = pd.DataFrame(result_list, columns=columns)
                result_df.to_csv(result_file, index=False)
                if items:
                    app_logger.info("Scraping %s postcode completed" % postcode)
                else:
                    app_logger.info("Scraping %s postcode completed, but it discovered no entries" % postcode)

                # sleep (5, 60) seconds randomly
                secs = random.randint(5, 60)
                app_logger.info("Sleeping %d seconds" % secs)
                time.sleep(secs)
