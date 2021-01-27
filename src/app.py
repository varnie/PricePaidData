import argparse

from constants import INIT_ACTION_NAME, QUERY_POSTCODES_ACTION_NAME
from logger import app_logger
from processor import PriceDataProcessor
from searcher import PostCodesSearcher

parser = argparse.ArgumentParser()
parser.add_argument('--action', required=True, choices=[INIT_ACTION_NAME, QUERY_POSTCODES_ACTION_NAME])


def init_handler():

    processor = PriceDataProcessor()
    if processor.fetch_data():
        if processor.get_errors():
            problem_urls = processor.get_problem_urls()
            app_logger.warning("The following urls were not downloaded: %s" % ", ".join(problem_urls))

        processor.preprocess_data()
    else:
        app_logger.error("All downloads failed")


def query_postcodes_handler():
    searcher = PostCodesSearcher()
    searcher.run()


if __name__ == "__main__":
    args = parser.parse_args()
    action = args.action

    if action == INIT_ACTION_NAME:
        init_handler()
    elif action == QUERY_POSTCODES_ACTION_NAME:
        query_postcodes_handler()
