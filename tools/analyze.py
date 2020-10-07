import csv
from datetime import datetime
from collections import OrderedDict
from tools.memory_logger import memory_logger

datetime_format = '%Y-%m-%d %H:%M:%S'


class LogLine:
    def __init__(self, log_line):
        self.username, self.ip_address, self.timestamp = log_line
        if isinstance(self.timestamp, str):
            self.timestamp = datetime.strptime(self.timestamp, datetime_format)

    def get_list_of_(self):
        return [self.username, self.ip_address, self.timestamp]


class MessageStorage:
    def __init__(self, csv_writer: csv.writer, seconds_offset: int = 3600):
        self.buffer = OrderedDict()
        self.last_added_log_line = None
        self.seconds_offset = seconds_offset
        self.csv_writer = csv_writer

    def pretty_writer(self, ip_address: str, write_items: list):
        """ip_address; start; stop; (username:login_time, username:login_time)"""

        start = write_items[0].timestamp
        end = write_items[-1].timestamp
        pretty_items = [f"{item.username}:{item.timestamp.strftime(datetime_format)}" for item in write_items]

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


@memory_logger
def analyze_sessions(csv_input_filename: str, output_csv_filename: str, session_time_offset=3600):
    print('Start analyzing...')

    with open(output_csv_filename, 'w') as file:
        csv_writer = csv.writer(file, quoting=csv.QUOTE_ALL)
        buffer = MessageStorage(csv_writer, session_time_offset)

        with open(csv_input_filename) as input_file:
            csv_reader = csv.reader(input_file)
            for record in csv_reader:
                buffer.push(LogLine(record))
