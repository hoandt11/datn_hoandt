import configparser
import re
import xml.etree.ElementTree as ET
import csv
from datetime import datetime
import os
import glob
import gzip
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor

# Định nghĩa namespace
NS = {'meas': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec'}

SOURCE_DIR = r"/opt/airflow/data/oss-enodeb_10.53.138.6_pmic2/source"
OUTPUT_DIR1 = r"/opt/airflow/data/oss-enodeb_10.53.138.6_pmic2/archive"
OUTPUT_DIR2 = r"/opt/airflow/data/oss-enodeb_10.53.138.6_pmic2/output"
LOG_FILE = r"/opt/airflow/data/oss-enodeb_10.53.138.6_pmic2/logs/extraction_log.xlsx"
STAGING_DIR = r"/opt/airflow/data/oss-enodeb_10.53.138.6_pmic2/staging"
CONF_FILE = r"/opt/airflow/data/oss-enodeb_10.53.138.6_pmic2/oss-enodeb.conf"

# Số lượng luồng tối đa
MAX_WORKERS = 4

# Khóa để đồng bộ hóa dữ liệu khi chạy đa luồng
data_lock = threading.Lock()
log_lock = threading.Lock()

# Tạo các thư mục nếu chưa tồn tại
for directory in [STAGING_DIR, OUTPUT_DIR1, OUTPUT_DIR2]:
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
            print(f"Created directory: {directory}")
        except PermissionError:
            print(f"Error: No permission to create directory {directory}")
            exit(1)
        except Exception as e:
            print(f"Error creating directory {directory}: {e}")
            exit(1)

# Đọc file log để kiểm tra file đã xử lý và lấy FOLDER_NUMBER
processed_files = set()
folder_number = 0
if os.path.exists(LOG_FILE):
    try:
        log_df = pd.read_excel(LOG_FILE)
        processed_files = set(log_df['FILE_NAME'].astype(str).tolist())
        if not log_df.empty:
            folder_number = int(log_df['FOLDER_NUMBER'].max())
        print(f"Loaded log file. Last FOLDER_NUMBER: {folder_number}")
    except Exception as e:
        print(f"Error reading log file {LOG_FILE}: {e}. Starting with empty log.")
        log_df = pd.DataFrame(columns=['STT', 'FOLDER_NAME', 'FILE_NAME', 'TIME_PROCESS', 'FOLDER_NUMBER'])
else:
    log_df = pd.DataFrame(columns=['STT', 'FOLDER_NAME', 'FILE_NAME', 'TIME_PROCESS', 'FOLDER_NUMBER'])
    print(f"Log file {LOG_FILE} not found. Creating new log.")

# Tăng FOLDER_NUMBER cho lần trích xuất mới
folder_number += 1
output1_subdir = os.path.join(OUTPUT_DIR1, str(folder_number))
if not os.path.exists(output1_subdir):
    try:
        os.makedirs(output1_subdir)
        print(f"Created subdirectory: {output1_subdir}")
    except PermissionError:
        print(f"Error: No permission to create directory {output1_subdir}")
        exit(1)
    except Exception as e:
        print(f"Error creating directory {output1_subdir}: {e}")
        exit(1)

# Đọc file cấu hình .conf
config = configparser.ConfigParser()
try:
    config.read(CONF_FILE)
except Exception as e:
    print(f"Error reading oss-enodeb.conf: {e}")
    exit(1)

# Lấy danh sách các section không thuộc nhóm cố định
sections = [s for s in config.sections() if s not in ['common', 'csv', 'routing']]
print(f"Sections found in .conf file: {sections}")
if not sections:
    print("Warning: No valid sections found in oss-enodeb.conf. No CSV files will be generated.")
    exit(1)

# Tạo từ điển để lưu thông tin mỗi section
section_data = {}
for s in sections:
    try:
        regexp = config.get(s, 'regexp')
        mt_map = config.get(s, 'mt_map').split(',')
        section_data[s] = {'regexp': regexp, 'mt_map': mt_map, 'rows': []}
        print(f"Section {s}: regexp={regexp}, mt_map={mt_map}")
    except configparser.NoOptionError as e:
        print(f"Error in section {s}: Missing required option {e}")
        exit(1)

# Quét tất cả các file .xml.gz trong SOURCE_DIR
xml_gz_files = glob.glob(os.path.join(SOURCE_DIR, "*.xml.gz"))
if not xml_gz_files:
    print(f"Error: No .xml.gz files found in {SOURCE_DIR}")
    exit(1)
print(f"Found {len(xml_gz_files)} .xml.gz files in {SOURCE_DIR}")

# Danh sách để lưu thông tin log
log_entries = []
stt = len(log_df) + 1 if not log_df.empty else 1

# Hàm xử lý một file .xml.gz
def process_xml_gz_file(xml_gz_file):
    file_name = os.path.basename(xml_gz_file)
    if file_name in processed_files:
        print(f"File {file_name} already processed. Skipping...")
        return

    print(f"Processing file: {xml_gz_file}")

    # Giải nén file .xml.gz vào thư mục staging
    xml_file = os.path.join(STAGING_DIR, file_name.replace('.xml.gz', '.xml'))
    try:
        with gzip.open(xml_gz_file, 'rb') as f_in, open(xml_file, 'wb') as f_out:
            f_out.write(f_in.read())
    except gzip.BadGzipFile:
        print(f"Error: File {xml_gz_file} is not a valid .gz file. Skipping...")
        return
    except Exception as e:
        print(f"Error decompressing {xml_gz_file}: {e}. Skipping...")
        return

    # Phân tích file XML từ file tạm
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
    except ET.ParseError:
        print(f"Error: Invalid XML format in {xml_file}. Skipping...")
        return
    finally:
        # Xóa file tạm sau khi xử lý
        if os.path.exists(xml_file):
            try:
                os.remove(xml_file)
                print(f"Deleted temporary file: {xml_file}")
            except Exception as e:
                print(f"Error deleting temporary file {xml_file}: {e}")

    # Trích xuất thông tin cố định
    measCollec = root.find('.//meas:measCollec', NS)
    if measCollec is None:
        print(f"Error: Không tìm thấy thẻ <measCollec> trong {xml_gz_file}. Skipping...")
        return
    ROPTime01 = measCollec.get('beginTime')
    dt = datetime.fromisoformat(ROPTime01.replace('+00:00', ''))
    ROPTime01 = dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f"ROPTIME01: {ROPTime01}")

    fileHeader = root.find('.//meas:fileHeader', NS)
    if fileHeader is None:
        print(f"Error: Không tìm thấy thẻ <fileHeader> trong {xml_gz_file}. Skipping...")
        return
    dnPrefix = fileHeader.get('dnPrefix')

    managedElement = root.find('.//meas:managedElement', NS)
    if managedElement is None:
        print(f"Error: Không tìm thấy thẻ <managedElement> trong {xml_gz_file}. Skipping...")
        return
    localDn = managedElement.get('localDn')
    COL2 = dnPrefix + ',' + localDn
    print(f"COL2: {COL2}")

    # Duyệt qua các phần tử measInfo
    meas_info_count = 0
    local_rows = {s: [] for s in sections}
    for measInfo in root.findall('.//meas:measInfo', NS):
        meas_info_count += 1
        granPeriod = measInfo.find('meas:granPeriod', NS)
        if granPeriod is None:
            print(f"Warning: Không tìm thấy granPeriod trong một measInfo của {xml_gz_file}. Skipping...")
            continue
        ROPTime02 = granPeriod.get('endTime')
        dt = datetime.fromisoformat(ROPTime02.replace('+00:00', ''))
        ROPTime02 = dt.strftime('%Y-%m-%d %H:%M:%S')
        COL1 = granPeriod.get('duration')
        print(f"Processing measInfo {meas_info_count}: ROPTime02={ROPTime02}, COL1={COL1}")

        # Tạo ánh xạ từ tên đo lường sang chỉ số p
        measTypes = {mt.text: mt.get('p') for mt in measInfo.findall('meas:measType', NS)}
        print(f"Measurement types found: {list(measTypes.keys())}")

        # Duyệt qua các phần tử measValue
        for measValue in measInfo.findall('meas:measValue', NS):
            measObjLdn = measValue.get('measObjLdn')
            print(f"Checking measObjLdn: {measObjLdn}")
            r_values = {r.get('p'): r.text for r in measValue.findall('meas:r', NS)}

            # Kiểm tra từng section trong file .conf
            for s in sections:
                if re.search(section_data[s]['regexp'], measObjLdn):
                    print(f"Match found for section {s} with measObjLdn: {measObjLdn}")
                    # Tạo hàng dữ liệu với 5 cột cố định
                    row = [ROPTime01, ROPTime02, COL1, COL2, measObjLdn]
                    # Thêm các giá trị đo lường từ mt_map
                    for mt in section_data[s]['mt_map']:
                        p = measTypes.get(mt)
                        value = r_values.get(p, '') if p else ''
                        row.append(value)
                    local_rows[s].append(row)
                    print(f"Added row to {s}: {row}")

    # Đồng bộ dữ liệu vào section_data
    with data_lock:
        for s in sections:
            section_data[s]['rows'].extend(local_rows[s])

    print(f"Total measInfo processed in {xml_gz_file}: {meas_info_count}")

    # Thêm thông tin vào log
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = {
        'STT': stt,
        'FOLDER_NAME': SOURCE_DIR,
        'FILE_NAME': file_name,
        'TIME_PROCESS': current_time,
        'FOLDER_NUMBER': folder_number
    }
    with log_lock:
        log_entries.append(log_entry)
        globals()['stt'] += 1

# Sử dụng ThreadPoolExecutor để xử lý đa luồng
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    executor.map(process_xml_gz_file, xml_gz_files)

# Ghi dữ liệu vào các file CSV
for s in sections:
    if not section_data[s]['rows']:
        print(f"No data for section {s}. Skipping CSV creation.")
        continue

    # Ghi vào output1 (thư mục con với FOLDER_NUMBER)
    csv_filename1 = os.path.join(output1_subdir, s + '.csv')
    print(f"Creating CSV file in output1: {csv_filename1}")
    try:
        with open(csv_filename1, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter='|', quoting=csv.QUOTE_NONE, escapechar='\\')
            for row in section_data[s]['rows']:
                writer.writerow(row)
        print(f"CSV file {csv_filename1} created with {len(section_data[s]['rows'])} rows")
    except PermissionError:
        print(f"Error: No permission to write to {csv_filename1}")
        exit(1)
    except Exception as e:
        print(f"Error writing to {csv_filename1}: {e}")
        exit(1)

    # Ghi vào output2 (ghi đè file cũ)
    csv_filename2 = os.path.join(OUTPUT_DIR2, s + '.csv')
    print(f"Creating CSV file in output2: {csv_filename2}")
    try:
        with open(csv_filename2, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter='|', quoting=csv.QUOTE_NONE, escapechar='\\')
            for row in section_data[s]['rows']:
                writer.writerow(row)
        print(f"CSV file {csv_filename2} created with {len(section_data[s]['rows'])} rows")
    except PermissionError:
        print(f"Error: No permission to write to {csv_filename2}")
        exit(1)
    except Exception as e:
        print(f"Error writing to {csv_filename2}: {e}")
        exit(1)

# Xóa các file trong thư mục staging (không xóa thư mục staging)
if os.path.exists(STAGING_DIR):
    for file_name in os.listdir(STAGING_DIR):
        file_path = os.path.join(STAGING_DIR, file_name)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Deleted file in staging: {file_path}")
        except Exception as e:
            print(f"Error deleting file {file_path} in staging: {e}")

# Cập nhật file log
if log_entries:
    new_log_df = pd.DataFrame(log_entries)
    log_df = pd.concat([log_df, new_log_df], ignore_index=True)
    try:
        log_df.to_excel(LOG_FILE, index=False)
        print(f"Updated log file {LOG_FILE} with {len(log_entries)} new entries")
    except PermissionError:
        print(f"Error: No permission to write to {LOG_FILE}")
        exit(1)
    except Exception as e:
        print(f"Error writing to {LOG_FILE}: {e}")
        exit(1)
else:
    print("No new files processed. Log file not updated.")