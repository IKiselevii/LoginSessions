import csv
from datetime import datetime
from collections import OrderedDict
import time
import tracemalloc

datetime_format = '%Y-%m-%d %H:%M:%S'


class LogLine:
    def __init__(self, log_line):
        self.username, self.ip_address, self.timestamp = log_line
        if isinstance(self.timestamp, str):
            self.timestamp = datetime.strptime(self.timestamp, datetime_format)

    def get_list_of_(self):
        return [self.username, self.ip_address, self.timestamp]


class MessageStorage:
    def __init__(self, output_filename: str, seconds_offset: int = 3600):
        self.buffer = OrderedDict()
        self.last_added_log_line = None
        self.seconds_offset = seconds_offset
        self.output_file = open(output_filename, 'w')
        self.csv_writer = csv.writer(self.output_file, quoting=csv.QUOTE_ALL)

    def pretty_writer(self, ip_address: str, write_items: list):
        """ip_address; start; stop; (username:login_time, username:login_time)"""

        pretty_items = list()
        start = datetime.now()
        end = datetime.fromtimestamp(0)
        for item in write_items:

            if item.timestamp < start:
                start = item.timestamp
            elif item.timestamp > end:
                end = item.timestamp

            pretty_items.append(f"{item.username}:{item.timestamp.strftime(datetime_format)}")

        self.csv_writer.writerow([ip_address, start, end, ','.join(pretty_items)])

    def clean_buffer(self, keys: list):

        for key in keys:
            del self.buffer[key]

    def check_buffer(self):

        delete_keys = []

        for key, value in self.buffer.items():
            if (self.last_added_log_line.timestamp - value[-1].timestamp).seconds > self.seconds_offset:
                if len(value) > 1:
                    self.pretty_writer(key, value)
                delete_keys.append(key)
            else:
                break

        self.clean_buffer(delete_keys)

    def push(self, record: LogLine):

        if self.buffer.get(record.ip_address):
            self.buffer[record.ip_address].append(record)
            self.buffer.move_to_end(record.ip_address, last=True)
        else:
            self.buffer[record.ip_address] = [record]

        self.last_added_log_line = record

        self.check_buffer()


def analyze_sessions(csv_input_filename: str, output_csv_filename: str, session_time_offset=3600):
    print('Start analyzing...')
    tracemalloc.start()
    start_time = time.time()

    buffer = MessageStorage(output_csv_filename, session_time_offset)

    with open(csv_input_filename) as input_file:
        csv_reader = csv.reader(input_file)
        for record in csv_reader:
            buffer.push(LogLine(record))

    buffer.output_file.close()
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    print('Analyze in:', round(time.time() - start_time, 1), f'seconds. Peak memory usage was: {peak / 10 ** 6}')

