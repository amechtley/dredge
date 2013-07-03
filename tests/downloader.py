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

Module to test dredge.downloader. The test methods here use boardgamegeek.com,
so may be susceptible to unexpected changes at a later date.
"""

import csv
import os
import re
import shutil
import unittest
import dredge.tests
import dredge.downloader


class TestMassDownloadXML(unittest.TestCase):
    """
    A class to test the mass_download() method with xml documents.
    """
    def setUp(self):
        """
        Call the method to get some test data.
        """
        self.game_ids = (1, 12, 123, 1234, 12345)
        self.temp_directory = dredge.tests.get_temp_directory()
        url_template =\
            'http://boardgamegeek.com/xmlapi2/thing?type=boardgame&id={id}'
        dredge.downloader.mass_download(
            item_ids=self.game_ids,
            url_template=url_template,
            url_format_expression=lambda game_id: {'id': game_id},
            output_directory=self.temp_directory,
            file_extension='xml'
        )

    def tearDown(self):
        """
        Clean up the temp directory.
        """
        shutil.rmtree(self.temp_directory)

    def test_error_log_exists(self):
        """
        Ensure the error log is properly created.
        """
        self.assertEqual(
            os.path.exists(
                os.path.join(
                    self.temp_directory, dredge.downloader.ERROR_LOG_NAME
                )
            ),
            True
        )

    def test_all_files_attempted(self):
        """
        Ensure all the files were either downloaded or attempted.
        """
        path_to_error_log = os.path.join(
            self.temp_directory, dredge.downloader.ERROR_LOG_NAME
        )
        with open(path_to_error_log) as f:
            reader = csv.DictReader(f)
            rows = tuple(row for row in reader)
        file_match = re.compile('\d+[.]xml')
        id_match = re.compile('\d+(?=[.]xml)')
        ids = tuple(
            sorted(
                [int(row['id']) for row in rows] + [
                    int(id_match.match(f).group(0))
                    for f in os.listdir(self.temp_directory)
                    if file_match.match(f)
                ]
            )
        )
        self.assertEqual(ids, self.game_ids)


class TestMassDownloadMultiPageHTML(unittest.TestCase):
    """
    A class to test the mass_download() method with multi-page html documents.
    """
    def setUp(self):
        """
        Call the method to get some test data.
        """
        self.user_ids = {384545: 'Orangemoose', 14687: 'Scuba'}
        self.temp_directory = dredge.tests.get_temp_directory()
        segment_url_template = \
            'http://boardgamegeek.com/collection/user/{user_name}?page={page}'
        dredge.downloader.mass_download(
            item_ids=self.user_ids.keys(),
            url_template='http://boardgamegeek.com/collection/user/{user_name}',
            url_format_expression=lambda user_id: {
                'user_name': self.user_ids[user_id]
            },
            output_directory=self.temp_directory,
            file_extension='html',
            segment_url_template=segment_url_template,
            segment_url_format_expression=lambda user_id, page_number: (
                {'user_name': self.user_ids[user_id], 'page': page_number}
            ),
            get_max_page_expression=lambda soup: max(
                int(x.text) for x in soup.find(
                    'span', class_='geekpages'
                ).find_all(lambda tag: tag.name == 'a' and tag.text.isnumeric())
            )
        )

    def tearDown(self):
        """
        Clean up the temp directory.
        """
        shutil.rmtree(self.temp_directory)

    # TODO: Add test case for something that won't change to verify page counts
    def test_all_ids_attempted(self):
        """
        Ensure all the files were either downloaded or attempted.
        """
        path_to_error_log = os.path.join(
            self.temp_directory, dredge.downloader.ERROR_LOG_NAME
        )
        with open(path_to_error_log) as f:
            reader = csv.DictReader(f)
            rows = tuple(row for row in reader)
        file_match = re.compile('\d+-\d+[.]html')
        id_match = re.compile('\d+')
        ids = tuple(
            sorted(
                set(
                    [int(row['id']) for row in rows] + [
                        int(id_match.match(f).group(0))
                        for f in os.listdir(self.temp_directory)
                        if file_match.match(f)
                    ]
                )
            )
        )
        expected = tuple(sorted(self.user_ids.keys()))
        self.assertEqual(ids, expected)


if __name__ == '__main__':
    unittest.main()