import requests
import os
import concurrent.futures
import sys
import re

# This script works as a 10-K web scraper of SEC filings on EDGAR

download_sub_dir_files = "download"
download_sub_dir_index = "index_files"
url_postfix = 'https://www.sec.gov/Archives/'


def parse_form_index(index_form_file, year, qtr):
    counter = 0

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)

    f = open(index_form_file, 'r')
    for line in f.readlines()[10:]:
        form_type = line[:12].strip()
        # company_name = line[12:74].strip()
        # cik = line[74:86].strip()
        date_filed = line[86:96].strip()
        rel_file_name = line[98:].strip()
        url = url_postfix + rel_file_name
        if form_type == "10-K":
            counter = counter + 1
            # download_file(url, rel_file_name, date_filed)
            executor.submit(download_file, url, rel_file_name,
                            date_filed, counter, year, qtr)

    print("Waiting for threads to finish...")
    executor.shutdown(wait=True)
    f.close()


def download_file(url, rel_file_name, date_filed, count, year, qtr):
    abs_file_path = os.path.abspath(
        download_sub_dir_files + "/" + year + "/" + qtr + "/" + date_filed + rel_file_name[10:])

    if os.path.isfile(abs_file_path):
        print(str(count) + ": " + abs_file_path +
              " already exists (" + str(os.path.getsize(abs_file_path)) + ").")
        return

    abs_dir = os.path.dirname(abs_file_path)
    if not os.path.exists(abs_dir):
        os.makedirs(abs_dir)

    print(str(count) + " Downloading " + url)

    r = requests.get(url)
    open(abs_file_path, 'wb').write(r.content)


def download_index_file(year, qtr):
    # https://www.sec.gov/Archives/edgar/full-index/2016/QTR4/form.idx
    url = url_postfix + "edgar/full-index/" + year + "/" + qtr + "/form.idx"
    print("Downloading index file: " + url)
    r = requests.get(url)
    abs_file_path = os.path.abspath(
        download_sub_dir_index + "/" + year + "/" + qtr + "/" + "form.idx")
    abs_dir = os.path.dirname(abs_file_path)
    if not os.path.exists(abs_dir):
        os.makedirs(abs_dir)
    open(abs_file_path, 'wb').write(r.content)
    return abs_file_path


def main():
    if len(sys.argv) < 3:
        print("Usage: 10_k_crawler3 <YEAR> <QTR[1-4]>")
        return

    year = sys.argv[1]
    qtr = sys.argv[2]

    qtr_pattern = re.compile("QTR[1-4]")
    year_pattern = re.compile(r"^\d{4}$")

    if qtr_pattern.match(qtr) is None or year_pattern.match(year) is None:
        print("Usage: 10_k_crawler3 <YEAR> <QTR[1-4]>")
        return

    index_form_file = download_index_file(year, qtr)
    parse_form_index(index_form_file, year, qtr)


if __name__ == '__main__':
    main()

