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

This module contains multiprocessing utilities.
"""

import csv
import functools
import itertools
import multiprocessing
import os
import sys
import traceback

# increase csv field size limit
csv.field_size_limit(sys.maxsize)

## the number of cores to use for job scheduling
CPU_COUNT = multiprocessing.cpu_count()


def do_multi_parse_to_csv(
        file_paths, output_folder, task_name, parser_func,
        cores_to_reserve=1, delimiter=',', are_ids_unique=True
):
    """
    Parse a collection of files across multiple processes and dump the output
        into a csv.
    @param file_paths: A collection of file paths containing the data.
    @param output_folder: Location where the results should be written.
    @param task_name: The name to give to the csv output.
    @param parser_func: A function with the signature func(path_to_file) that
        returns a namedtuple object containing a primary key, id, or a
        collection of such objects.
    @param cores_to_reserve: The number of cores to leave idle.
    @param delimiter: Delimiter to use in csv output.
    @param are_ids_unique: True if there is a unique id column; otherwise,
        False.
    """
    # balance the load across all tasks
    task_count = get_num_tasks(cores_to_reserve, file_paths)
    sorted_file_paths = sort_file_paths_for_load_balancing(
        file_paths, task_count
    )
    # get the csv headers by just parsing a test file
    test_entry = parser_func(sorted_file_paths[0])
    cls = test_entry.__class__
    if hasattr(test_entry, '_fields'):
        csv_headers = test_entry._fields
    else:
        csv_headers = test_entry[0]._fields
    # ensure the output directory exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    # do the multiprocess
    do_multi_process(
        sorted_file_paths,
        functools.partial(
            _dump_into_csv_task,
            output_folder=output_folder,
            task_name=task_name,
            parser_func=parser_func,
            csv_headers=csv_headers,
            delimiter=delimiter
        ),
        cores_to_reserve=cores_to_reserve
    )
    # stitch files together
    ids = set()
    lines = list()
    error_lines = list()
    for i in xrange(task_count):
        path_to_csv = os.path.join(output_folder, '%s-%i.csv' % (task_name, i))
        with open(path_to_csv) as csv_file:
            for row in csv.reader(csv_file, delimiter=delimiter):
                entry = cls(*row)
                if not are_ids_unique or entry.id not in ids:
                    lines.append(row)
                    ids.add(entry.id)
        path_to_error_log = os.path.join(
            output_folder, '%s-%i-errors.csv' % (task_name, i)
        )
        with open(path_to_error_log) as csv_file:
            error_lines += [row for row in csv.reader(csv_file)][1:]
    path_to_final_output = os.path.join(output_folder, '%s.csv' % task_name)
    with open(path_to_final_output, 'w+') as csv_file:
        csv.writer(csv_file, delimiter=delimiter).writerows(lines)
    path_to_final_error_log = os.path.join(
        output_folder, '%s-errors.csv' % task_name
    )
    with open(path_to_final_error_log, 'w+') as csv_file:
        csv.writer.writerow(['file', 'error'])
        csv.writer(csv_file).writerows(error_lines)


def do_multi_process(data, task, cores_to_reserve=1, **kwargs):
    """
    Perform a task on a tuple of data over all available processors.
    @param data: A tuple of data to process.
    @param task: A task with the signature:
        (data, slice_start, slice_end, result_queue, kwargs). The keyword
        argument '_task_index' is also sent to each task.
    @param cores_to_reserve: The number of cores to leave idle.
    @param kwargs: Any additional keyword arguments for task.
    @return: A list containing all of the workers' results.
    """
    # determine how to cut up work load
    num_tasks = get_num_tasks(cores_to_reserve, data)
    slice_ranges = get_multiprocess_slice_ranges(num_tasks, len(data))
    # start a worker for each CPU
    results_queue = multiprocessing.Queue()
    consumers = [
        multiprocessing.Process(
            target=task,
            args=(data, slice_ranges[x][0], slice_ranges[x][1], results_queue),
            kwargs=dict(kwargs.items() + [('_task_index', x)])
        ) for x in xrange(num_tasks)
    ]
    for worker in consumers:
        worker.start()
    results = list()
    while num_tasks:
        result = results_queue.get()
        results.append(result)
        num_tasks -= 1
    return results


def _dump_into_csv_task(
        file_paths, slice_start, slice_end, result_queue,
        output_folder, task_name, csv_headers, delimiter, parser_func,
        **kwargs
):
    """
    A task to parse a collection of files and dump the data into a csv.
    @param file_paths: Full paths to all of the files being parsed.
    @param slice_start: The start for the range to be parsed.
    @param slice_end: The end of the range to be parsed.
    @param result_queue: The queue into which the result should be placed.
    @param output_folder: Location where the results should be written.
    @param task_name: Name to be given to output files.
    @param csv_headers: Headers for the csv output.
    @param delimiter: Delimiter to use in csv output.
    @param parser_func: A function with the signature func(path_to_file) that
        returns a namedtuple object whose 0th element is a primary key, or a
        collection of such objects.
    @param kwargs: Method signature requirement.
    """
    # create csv if it doesn't exist
    path_to_csv = os.path.join(
        output_folder, '%s-%i.csv' % (task_name, kwargs['_task_index'])
    )
    if not os.path.exists(path_to_csv):
        with open(path_to_csv, 'w+') as csv_file:
            csv.writer(csv_file, delimiter=delimiter).writerow(csv_headers)
    # create error log if it doesn't exist
    error_path = os.path.join(
        output_folder, '%s-%i-errors.csv' % (task_name, kwargs['_task_index'])
    )
    if not os.path.exists(error_path):
        with open(error_path, 'w+') as error_file:
            csv.writer(error_file).writerow(['file', 'error'])
    # write each entry to the csv
    for i in xrange(slice_start, slice_end):
        file_path = file_paths[i]
        try:
            entry = parser_func(file_path)
        except Exception:
            tb = traceback.format_exc()
            with open(error_path, 'a+') as error_file:
                csv.writer(error_file).writerow([file_path, tb])
            continue
        with open(path_to_csv, 'a+') as csv_file:
            if hasattr(entry, '_fields'):
                csv.writer(csv_file, delimiter=delimiter).writerow(entry)
            else:
                csv.writer(csv_file, delimiter=delimiter).writerows(entry)
    # rejoin the main thread
    result_queue.put(path_to_csv)


def get_multiprocess_slice_ranges(num_tasks, data_count):
    """
    Gets the slice ranges for cutting up a multiprocess job.
    @param num_tasks: The number of tasks to divide the data over.
    @param data_count: The size of the tuple to be multiprocessed.
    @return: A tuple of tuples that are (slice_start, slice_end).
    """
    slice_size = data_count / num_tasks
    remainder = data_count % num_tasks
    return tuple(
        (
            x * slice_size + (x if x < remainder else remainder),
            (x + 1) * slice_size + (x + 1 if x < remainder else remainder)
        )
        for x in xrange(num_tasks)
    )


def get_num_tasks(cores_to_reserve, data):
    """
    Get the number of tasks based on the desired parameters.
    @param cores_to_reserve: Number of cores to not set on the process.
    @param data: The data to be processed.
    @return: The number of tasks over which the process will be distributed.
    """
    return min(max(CPU_COUNT - cores_to_reserve, 1), len(data))


def sort_file_paths_for_load_balancing(file_paths, task_count):
    """
    Sort a collection of file paths for proper load balancing.
    @param file_paths: A collection of file paths for e.g., XML documents.
    @return: A tuple of file paths sorted for load balancing based on file size.
    """
    # sort files by size
    file_paths = sorted(
        file_paths,
        cmp=lambda x, y: -cmp(os.stat(x).st_size, os.stat(y).st_size)
    )
    # balance the load across all tasks
    sorted_file_paths = [list() for _ in xrange(task_count)]
    for i in xrange(0, len(file_paths), task_count):
        for j in xrange(0, task_count):
            try:
                sorted_file_paths[j].append(file_paths[i+j])
            except IndexError:
                pass
    return tuple(itertools.chain.from_iterable(sorted_file_paths))