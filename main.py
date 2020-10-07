from tools.sorter import batch_sort
from tools.analyze import analyze_sessions

storage_dir = '.batches/'
main_path = 'logins0.csv'
output_file = 'logins0_sorted.csv'
analytics_output_file = 'detected_sessions.csv'

batch_sort(main_path, output_file)  # Sorted in: 29.1 seconds. Peak memory usage was 2.684969MB
analyze_sessions(output_file, analytics_output_file, 3600)  # Analyze in: 13.4 seconds. Peak memory usage was: 0.737906
