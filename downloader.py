"""
The MIT License (MIT)

Copyright (c) 2013 Adam Mechtley

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

This module contains methods for performing mass downloads.
"""

import bs4
import csv
import os
import re
import time
import traceback
import urllib2


## name of a csv file to dump info about items for which there were errors
ERROR_LOG_NAME = 'errors.csv'


def mass_download(
        item_ids, url_template, url_format_expression, output_directory,
        file_extension='xml',
        download_burst_count=50, sleep_time=30,
        segment_url_template=None,
        segment_url_format_expression=None,
        get_max_page_expression=None
):
    """
    Downloads a bunch of data for the supplied items_ids using the supplied url
        template and formatting expression. The method is designed in a way that
        the process can be terminated and resumed later. It also accommodates
        downloading multi-page html documents with optional parameters.
    @param item_ids: Collection of ids specifying what is to be downloaded. This
        collection is usually numbers, but may be user names or something else.
    @param url_template: URL template with formatting entries. E.g.,
        http://boardgamegeek.com/xmlapi2/collection?user={name}
    @param url_format_expression: Lambda expression to generate url template
        kwargs from an id. E.g., lambda item_id: {'name': lookup_table[item_id]}
    @param output_directory: Directory where data should be stored.
    @param file_extension: Extension to use for downloaded data.
    @param download_burst_count: Number of downloads to execute in succession.
    @param sleep_time: Number of seconds to sleep between download bursts.
    @param segment_url_template: URL template for formatting segments on multi-
        page downloads. E.g.,
        http://boardgamegeek.com/collection/user/{user_name}?page={page_number}
    @param segment_url_format_expression: Lambda expression to generate url
        template kwargs from an item id and page number.
        E.g., lambda user_id, p: {
            'user_name': lookup_table[user_id], 'page_number': p
        }
    @param get_max_page_expression: Lambda expression to find the max page count
        for a multi-page download in a souped document.
        E.g., lambda soup: max(
            int(x) for x in re.findall(
                '\d+', soup.find('span', class_='geekpages').text
            )
        )
    """
    # create the output xml_directory if it does not already exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    # create the error file if it does not already exist
    path_to_error_log = os.path.join(output_directory, ERROR_LOG_NAME)
    error_items = list()
    if not os.path.exists(path_to_error_log):
        with open(path_to_error_log, 'w') as csv_file:
            csv.writer(csv_file).writerow(['id', 'exception'])
    else:
        with open(path_to_error_log) as csv_file:
            error_items = [row['id'] for row in csv.DictReader(csv_file)]
    # get list of already downloaded item_ids
    file_extension = re.search('[A-Za-z]+', file_extension).group(0)
    downloaded_file_name_match = re.compile('.*[.]' + file_extension + '$')
    existing_downloaded_data = set(
        [
            os.path.splitext(file_name)[0]
            for file_name in os.listdir(output_directory)
            if downloaded_file_name_match.match(file_name)
        ] + error_items
    )
    download_count = 0
    for item_id in item_ids:
        # skip already downloaded data
        if str(item_id) in existing_downloaded_data:
            continue
        # download the data
        url = url_template.format(**url_format_expression(item_id))
        try:
            downloaded_data = urllib2.urlopen(url).read()
            # code path if the data does not need to be parsed
            if get_max_page_expression is None:
                # save the data to a file
                path_to_file_on_disk = os.path.join(
                    output_directory, '%s.%s' % (item_id, file_extension)
                )
                with open(path_to_file_on_disk, 'w+') as file_on_disk:
                    file_on_disk.write(downloaded_data)
            # otherwise look for the page counter in the data
            else:
                # assume it's html
                soup = bs4.BeautifulSoup(urllib2.urlopen(url).read(), 'lxml')
                max_page = get_max_page_expression(soup)
                for page_number in xrange(1, max_page + 1):
                    # skip if a file has already been downloaded
                    file_name = '%s-%04i.html' % (item_id, page_number)
                    if file_name in os.listdir(output_directory):
                        continue
                    # download the individual page
                    page_url = segment_url_template.format(
                        **segment_url_format_expression(item_id, page_number)
                    )
                    html_data = urllib2.urlopen(page_url).read()
                    # save the data to a file
                    path_to_file_on_disk = os.path.join(
                        output_directory, file_name
                    )
                    with open(path_to_file_on_disk, 'w+') as file_on_disk:
                        file_on_disk.write(html_data)
        except Exception:
            print 'error with %s' % item_id
            tb = traceback.format_exc()
            with open(path_to_error_log, 'a') as csv_file:
                csv.writer(csv_file).writerow([item_id, tb])
        # wait between bursts
        download_count += 1
        if download_count % download_burst_count == 0:
            time.sleep(sleep_time)