import csv
from datetime import datetime
import time
import os
import shutil
import heapq
from collections import namedtuple
import tracemalloc
import re

storage_dir = '.batches/'
time_start = time.time()
timestamp_parsing_format = '%Y-%m-%d %H:%M:%S'
Keyed = namedtuple("Keyed", ["key", "obj"])


def split_into_batch(iterable, batch_length):
    elements_list = []
    for index, item in enumerate(iterable):
        elements_list.append(item)

        if len(elements_list) == batch_length:
            result_list = elements_list
            elements_list = []
            yield result_list

    if elements_list:
        yield elements_list


def merge(iterables):
    elements = [(Keyed(datetime.strptime(line[2], timestamp_parsing_format), line) for line in iterable)
                for iterable in iterables]

    for element in heapq.merge(*elements):
        yield element.obj


def merge_sort(batch_filenames: list, temp_dir: str):

    merging_files_readers = []

    for filename in batch_filenames:
        merging_files_readers.append(open(filename, 'r'))

    merged_batch_filename = temp_dir + re.sub(r'\.', '_', str(time.time())) + '.csv'
    with open(merged_batch_filename, 'w') as merged_file:
        merged_writer = csv.writer(merged_file)
        for item in merge([csv.reader(i) for i in merging_files_readers]):
            merged_writer.writerow(item)

    for reader in merging_files_readers:
        os.remove(reader.name)
        reader.close()

    return merged_batch_filename


def recursively_merge_sort(filename_list: list, temp_dir: str, offset_count=50):

    files_to_merging = []
    result_files = []

    for number, filename in enumerate(filename_list):
        files_to_merging.append(filename)
        if isinstance(number / offset_count, int):
            result_files.append(merge_sort(files_to_merging, temp_dir))
            files_to_merging = []

    if len(files_to_merging) != 0:
        result_files.append(merge_sort(files_to_merging, temp_dir))

    if len(result_files) > 1:
        output_filename = recursively_merge_sort(result_files, temp_dir, offset_count)
    else:
        return result_files[0]

    return output_filename


def batch_sort(sorting_filename, output_filename, batch_size=500, opened_files_limit=50, temp_dir=storage_dir):
    print('Sorting...')
    tracemalloc.start()

    batch_names = []
    merged_names = []

    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)

    if temp_dir[-1] != '/':
        temp_dir += '/'

    sorting_item = csv.reader(open(sorting_filename, newline=''))
    for index, item in enumerate(split_into_batch(sorting_item, batch_size)):

        item.sort(key=lambda k: datetime.strptime(k[2], timestamp_parsing_format))

        batch_filename = temp_dir + str(index) + '.csv'
        batch_names.append(batch_filename)

        with open(batch_filename, 'w+') as batch_file:

            csv_writer = csv.writer(batch_file)
            for i in item:
                csv_writer.writerow(i)

        if len(batch_names) == opened_files_limit:
            merged_names.append(merge_sort(batch_names, temp_dir))
            batch_names = []

    if batch_names:
        merged_names.append(merge_sort(batch_names, temp_dir))

    result_file = recursively_merge_sort(merged_names, temp_dir, opened_files_limit)
    shutil.move(result_file, output_filename)
    shutil.rmtree(temp_dir)

    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    print(f'Sorted in:', round(time.time() - time_start, 1), f'seconds. Peak memory usage was {peak / 10 ** 6}MB')




