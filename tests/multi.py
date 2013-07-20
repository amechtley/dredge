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

Module to test dredge.multi.
"""

import collections
import csv
import itertools
import lxml.etree
import os
import re
import unittest
import shutil
import dredge.multi
import dredge.tests

## paths to test files
_test_xml_files = [
    os.path.join(dredge.tests.TEST_FILES_FOLDER, '%02i.xml' % i)
    for i in range(1, 9)
]
## a simple type corresponding to the data in the test files
Note = collections.namedtuple(
    'Note', ['id', 'sender', 'recipient', 'message']
)
## expected results of parsing the xml data, sorted by id
_expected_xml_results = (
    Note(
        id=1, sender=u'Adam', recipient=u'Wadam',
        message=u'A message.'
    ),
    Note(
        id=2, sender=u'Adam', recipient=u'Wadam',
        message=u'A longer message.'
    ),
    Note(
        id=3, sender=u'Adam', recipient=u'Wadam',
        message=u'An even longer message.'
    ),
    Note(
        id=4, sender=u'Adam', recipient=u'Wadam',
        message=u'An even longer message than the last.'
    ),
    Note(
        id=5, sender=u'Adam', recipient=u'Wadam',
        message=u'An even longer message than the last one.'
    ),
    Note(
        id=6, sender=u'Adam', recipient=u'Wadam',
        message=u'An even longer message than the last one was.'
    ),
    Note(
        id=7, sender=u'Adam', recipient=u'Wadam',
        message=u'An even longer message than the last one was long.'
    ),
    Note(
        id=8, sender=u'Adam', recipient=u'Wadam',
        message=u'An even longer message than the last one was long. Please ' +
                u'stop reading.'
    )
)


def parser_func(file_path):
    """
    An example function with the required method signature for a csv parser
        function. Note that you could pass additional arguments using
        functools.partial().
    @param file_path: Path to a file to parse.
    """
    with open(file_path) as f:
        note = lxml.etree.fromstring(f.read())
    return Note(
        int(note.attrib['id']),
        unicode(note.find('from').text.encode('utf-8')),
        unicode(note.find('to').text.encode('utf-8')),
        unicode(note.find('message').text.encode('utf-8'))
    )


def parser_task(file_paths, slice_start, slice_end, result_queue, **kwargs):
    """
    An example function with the required method signature for a task. Note that
        it could include further keyword arguments as desired. Note also that
        kwargs will always contain _task_index.
    @param file_paths: A collection of paths to xml files.
    @param slice_start: The start for the range to be parsed.
    @param slice_end: The end of the range to be parsed.
    @param result_queue: The queue into which the result should be placed.
    @param kwargs: Method signature requirement.
    """
    results = list()
    for i in range(slice_start, slice_end):
        results.append(parser_func(file_paths[i]))
    result_queue.put(results)


# TODO: Test cases illustrating error collection
# TODO: Test cases illustrating non-unique ids
class TestDoMultiParseToCSV(unittest.TestCase):
    """
    Test the do_multi_parse_to_csv() method.
    """
    def setUp(self):
        """
        Call the method to create the test output files. Note that the output of
            parser_func is expected to be a namedtuple type with an attribute
            called 'id'.
        """
        self.temp_directory = dredge.tests.get_temp_directory()
        dredge.multi.do_multi_parse_to_csv(
            file_paths=_test_xml_files,
            output_folder=self.temp_directory,
            task_name='notes',
            parser_func=parser_func,
            cores_to_reserve=1,
            delimiter=',',
            id_column=0
        )

    def tearDown(self):
        """
        Clean up the temp directory.
        """
        shutil.rmtree(self.temp_directory)

    def test_intermediate_output(self):
        """
        Verify the proper number of intermediate files.
        """
        partial_files = [
            f for f in os.listdir(self.temp_directory)
            if re.match('notes-\d+.csv', f)
        ]
        error_files = [
            f for f in os.listdir(self.temp_directory)
            if re.match('notes-\d+-errors.csv', f)
        ]
        self.assertEqual(len(partial_files), len(error_files))

    def test_final_output(self):
        """
        Verify the final output file's contents.
        """
        with open(os.path.join(self.temp_directory, 'notes.csv')) as f:
            reader = csv.DictReader(f)
            rows = tuple(row for row in reader)
        entries = tuple(
            sorted(
                tuple(
                    Note(
                        int(row['id']),
                        row['sender'],
                        row['recipient'],
                        row['message']
                    )
                    for row in rows
                ),
                cmp=lambda x, y: cmp(x.id, y.id)
            )
        )
        self.assertEqual(entries, _expected_xml_results)


class TestDoMultiProcess(unittest.TestCase):
    """
    Test the do_multi_process() method.
    """
    def test_parse_xml_objects(self):
        """
        A test case demonstrating parsing xml objects.
        """
        notes = tuple(
            itertools.chain.from_iterable(
                dredge.multi.do_multi_process(
                    data=_test_xml_files,
                    task=parser_task,
                    cores_to_reserve=1
                )
            )
        )
        actual = tuple(sorted(notes, cmp=lambda x, y: cmp(x.id, y.id)))
        self.assertEqual(actual, _expected_xml_results)


class TestGetMultiprocessSliceRanges(unittest.TestCase):
    """
    Test the get_multiprocess_slice_ranges() method.
    """
    def test_even_ranges(self):
        """
        Should evenly divide up slice ranges when possible.
        """
        expected = [
            (0, 2),
            (2, 4),
            (4, 6),
            (6, 8),
            (8, 10),
            (10, 12),
            (12, 14),
            (14, 16)
        ]
        actual = dredge.multi.get_multiprocess_slice_ranges(8, 16)
        for i in xrange(8):
            self.assertEqual(expected[i], actual[i])

    def test_odd_final_ranges(self):
        """
        Final ranges should be short if tasks cannot be evenly divided up.
        """
        expected = (
            (0, 2),
            (2, 4),
            (4, 6),
            (6, 8),
            (8, 10),
            (10, 12),
            (12, 13),
            (13, 14)
        )
        actual = dredge.multi.get_multiprocess_slice_ranges(8, 14)
        self.assertEqual(expected, actual)


class TestMergeCSVFiles(unittest.TestCase):
    """
    Test the merge_csv_files() method.
    """
    def setUp(self):
        """
        SOME DESCRIPTION
        """
        self.temp_directory = dredge.tests.get_temp_directory()
        self.expected_headers = [('Id', 'Value')]
        self.expected_data = [
            ('0', '0'),
            ('1', '10'),
            ('2', '20'),
            ('3', '30'),
            ('4', '0'),
            ('5', '10'),
            ('6', '20'),
            ('7', '30'),
            ('8', '0'),
            ('9', '10'),
            ('10', '20'),
            ('11', '30'),
            ('12', '0'),
            ('13', '10'),
            ('14', '20'),
            ('15', '30')
        ]

    def tearDown(self):
        """
        Clean up the temp directory.
        """
        shutil.rmtree(self.temp_directory)

    def test_stitch(self):
        """
        Test stitching the test data.
        """
        output_path = os.path.join(self.temp_directory, 'merge.csv')
        dredge.multi.merge_csv_files(
            input_paths=[
                os.path.join(
                    dredge.tests.TEST_FILES_FOLDER, 'merge-%02i.csv' % i
                ) for i in range(4)
            ],
            output_path=output_path,
            delimiter=',',
            headers=self.expected_headers
        )
        with open(output_path) as csv_file:
            actual = tuple(tuple(row) for row in csv.reader(csv_file))
        expected = tuple(self.expected_headers + self.expected_data)
        self.assertEqual(actual, expected)

    def test_delimiter(self):
        """
        Test using a non-comma delimiter.
        """
        output_path = os.path.join(self.temp_directory, 'merge-tab_delimiter.csv')
        dredge.multi.merge_csv_files(
            input_paths=[
                os.path.join(
                    dredge.tests.TEST_FILES_FOLDER,
                    'merge-%02i-tab_delimiter.csv' % i
                ) for i in range(4)
            ],
            output_path=output_path,
            delimiter='\t',
            headers=self.expected_headers
        )
        with open(output_path) as csv_file:
            actual = tuple(
                tuple(row) for row in csv.reader(csv_file, delimiter='\t')
            )
        expected = tuple(self.expected_headers + self.expected_data)
        self.assertEqual(actual, expected)

    def test_without_headers(self):
        """
        Test a set of files with no headers
        """
        output_path = os.path.join(self.temp_directory, 'merge-no_headers.csv')
        dredge.multi.merge_csv_files(
            input_paths=[
                os.path.join(
                    dredge.tests.TEST_FILES_FOLDER,
                    'merge-%02i-no_headers.csv' % i
                ) for i in range(4)
            ],
            output_path=output_path,
            delimiter=',',
            headers=None
        )
        with open(output_path) as csv_file:
            actual = tuple(tuple(row) for row in csv.reader(csv_file))
        expected = tuple(self.expected_data)
        self.assertEqual(actual, expected)

    def test_unique_ids(self):
        """
        Test a set of files with a custom entry class to ensure ids are unique.
        """
        output_path = os.path.join(self.temp_directory, 'merge.csv')
        dredge.multi.merge_csv_files(
            input_paths=[
                os.path.join(
                    dredge.tests.TEST_FILES_FOLDER, 'merge-%02i.csv' % i
                ) for i in range(4)
            ] * 2,
            output_path=output_path,
            delimiter=',',
            headers=self.expected_headers,
            id_column=0
        )
        with open(output_path) as csv_file:
            actual = tuple(tuple(row) for row in csv.reader(csv_file))
        expected = tuple(self.expected_headers + self.expected_data)
        self.assertEqual(actual, expected)


class TestGetNumTasks(unittest.TestCase):
    """
    Test the get_num_tasks() method.
    """
    def setUp(self):
        """
        Create some test data.
        @return:
        """
        self.reserve_count = 2
        self.data = range(dredge.multi.CPU_COUNT * 2)
        self.small_data = range(1)

    def test_min(self):
        """
        Should return 1 task if too many reserve cores specified.
        """
        self.assertEqual(
            dredge.multi.get_num_tasks(dredge.multi.CPU_COUNT * 2, self.data), 1
        )

    def test_max(self):
        """
        Should equal cpu count if no cores are reserved.
        """
        self.assertEqual(
            dredge.multi.get_num_tasks(0, self.data), dredge.multi.CPU_COUNT
        )

    def test_reserve(self):
        """
        Should equal cpu less reserve count in all normal circumstances.
        """
        self.assertEqual(
            dredge.multi.get_num_tasks(self.reserve_count, self.data),
            dredge.multi.CPU_COUNT - self.reserve_count
        )

    def test_small_data(self):
        """
        Should return size of data to be processed if smaller than cpu count
            less reserve count.
        """
        self.assertEqual(
            dredge.multi.get_num_tasks(self.reserve_count, self.small_data),
            len(self.small_data)
        )


class TestSortFilePathsForLoadBalancing(unittest.TestCase):
    """
    Test the sort_file_paths_for_load_balancing() method.
    """
    def test_sorting_files(self):
        """
        Sort the test files assuming four tasks.
        """
        test_files_dir = os.path.join(os.path.dirname(__file__), 'files')
        expected = (
            os.path.join(test_files_dir, '08.xml'),  # 193 bytes +
            os.path.join(test_files_dir, '04.xml'),  # 149 bytes = 342 bytes
            os.path.join(test_files_dir, '07.xml'),  # 162 bytes +
            os.path.join(test_files_dir, '03.xml'),  # 135 bytes = 297 bytes
            os.path.join(test_files_dir, '06.xml'),  # 157 bytes +
            os.path.join(test_files_dir, '02.xml'),  # 129 bytes = 286 bytes
            os.path.join(test_files_dir, '05.xml'),  # 153 bytes +
            os.path.join(test_files_dir, '01.xml')   # 122 bytes = 275 bytes
        )
        actual = dredge.multi.sort_file_paths_for_load_balancing(_test_xml_files, 4)
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()