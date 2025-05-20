import configparser
import pandas as pd
import csv
import glob
import os
from datetime import datetime

# Cấu hình thư mục
RAW_DATA_DIR = r'/opt/airflow/data/huawei-lte_U2020-TAISHAN_host03/source'
OUTPUT1_DIR = r'/opt/airflow/data/huawei-lte_U2020-TAISHAN_host03/archive'
OUTPUT2_DIR = r'/opt/airflow/data/huawei-lte_U2020-TAISHAN_host03/output'
LOG_FILE = r'/opt/airflow/data/huawei-lte_U2020-TAISHAN_host03/logs/extraction_log.xlsx'
CONF_FILE = r'/opt/airflow/data/huawei-lte_U2020-TAISHAN_host03/huawei-lte_U2020-TAISHAN_host03.conf'


# Tạo thư mục đầu ra nếu chưa tồn tại
os.makedirs(OUTPUT1_DIR, exist_ok=True)
os.makedirs(OUTPUT2_DIR, exist_ok=True)

# Đọc file cấu hình .conf
config = configparser.ConfigParser()
conf_file = os.path.join(RAW_DATA_DIR, CONF_FILE)
config.read(conf_file)

# Đọc file log hiện tại (nếu có) để kiểm tra file đã xử lý
try:
    log_df = pd.read_excel(LOG_FILE)
    processed_files = set(log_df['FILE_NAME'])
    last_folder_number = log_df['FOLDER_NUMBER'].max() if not log_df.empty else 0
except (FileNotFoundError, KeyError):
    log_df = pd.DataFrame(columns=['FOLDER_NAME', 'FILE_NAME', 'TIME_PROCESS', 'FOLDER_NUMBER'])
    processed_files = set()
    last_folder_number = 0

# Tăng số folder cho output1 một lần duy nhất cho toàn bộ lần chạy
next_folder_number = last_folder_number + 1
output1_subdir = os.path.join(OUTPUT1_DIR, str(next_folder_number))
os.makedirs(output1_subdir, exist_ok=True)

# Lấy danh sách các type từ section [common]
typelist_str = config['common']['typelist']
typelist = typelist_str.split(',')

# Xử lý từng type trong typelist
for type_id in typelist:
    # Lấy section tương ứng với type
    section = config[type_id]
    
    # Lấy danh sách các cột cần trích xuất
    fields_str = section['fields']
    fields_list = next(csv.reader([fields_str]))
    
    # Tìm tất cả file CSV thô chứa type_id trong tên
    pattern = os.path.join(RAW_DATA_DIR, f'*_{type_id}_*.csv')
    files = glob.glob(pattern)
    
    if len(files) == 0:
        print(f"Không tìm thấy file nào cho {type_id}")
        continue
    
    # Khởi tạo DataFrame với kiểu dữ liệu mặc định
    dtypes = {field: 'object' for field in fields_list}
    df_combined = pd.DataFrame(columns=fields_list).astype(dtypes)
    
    # Xử lý từng file CSV thô
    for raw_file in files:
        file_name = os.path.basename(raw_file)
        if file_name in processed_files:
            print(f"Đã xử lý file {file_name}, bỏ qua")
            continue
        
        # Đọc file CSV thô, sử dụng dòng đầu tiên làm header và bỏ qua dòng thứ hai
        df = pd.read_csv(raw_file, header=0, skiprows=[1])
        
        # Tạo DataFrame mới với tất cả cột được chỉ định
        df_selected = pd.DataFrame(columns=fields_list).astype(dtypes)
        
        # Thêm từng cột vào DataFrame
        for field in fields_list:
            if field in df.columns:
                df_selected[field] = df[field].astype('object')
            else:
                df_selected[field] = pd.NA
                print(f"Cột {field} không tồn tại trong {raw_file}, đã tạo cột với giá trị null")
        
        # Tổng hợp dữ liệu vào df_combined
        df_combined = pd.concat([df_combined, df_selected], ignore_index=True)
    
    # Lưu vào output1 với định dạng không header, phân tách bằng '|'
    output1_file = os.path.join(output1_subdir, f'{type_id}.csv')
    df_combined.to_csv(output1_file, sep='|', header=False, index=False, quoting=csv.QUOTE_NONE, escapechar='\\')
    print(f"Đã trích xuất và tổng hợp dữ liệu thành công vào {output1_file} từ {len(files)} file")
    
    # Lưu vào output2, ghi đè nếu đã tồn tại
    output2_file = os.path.join(OUTPUT2_DIR, f'{type_id}.csv')
    df_combined.to_csv(output2_file, sep='|', header=False, index=False, quoting=csv.QUOTE_NONE, escapechar='\\')
    print(f"Đã ghi đè dữ liệu thành công vào {output2_file}")
    
    # Cập nhật log
    time_process = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    new_log_entries = pd.DataFrame({
        'FOLDER_NAME': [output1_subdir] * len(files),
        'FILE_NAME': [os.path.basename(f) for f in files],
        'TIME_PROCESS': [time_process] * len(files),
        'FOLDER_NUMBER': [next_folder_number] * len(files)
    })
    log_df = pd.concat([log_df, new_log_entries], ignore_index=True)
    log_df.to_excel(LOG_FILE, index=False)

print("Hoàn tất quá trình trích xuất và ghi log")