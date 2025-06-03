#!/usr/bin/env python3
"""
Script to generate a 10GB CSV file with customer records and upload to SFTP server.
Optimized with multiprocessing for faster generation.
"""

import csv
import os
import time
import random
import string
from datetime import datetime, timedelta
import paramiko
from paramiko import SSHClient
import logging
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import tempfile
import shutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SFTP_HOST = 'localhost'
SFTP_PORT = 30022
SFTP_USERNAME = 'airflow'
SFTP_PASSWORD = 'airflow123'
SFTP_REMOTE_PATH = '/upload/customers_10gb.csv'

LOCAL_FILE_PATH = 'customers_10gb.csv'
TARGET_SIZE_GB = 4
TARGET_SIZE_BYTES = TARGET_SIZE_GB * 1024 * 1024 * 1024

# Multiprocessing configuration
NUM_PROCESSES = min(mp.cpu_count(), 8)  # Use up to 8 cores
CHUNK_SIZE = 50000  # Records per chunk

class CustomerDataGenerator:
    """Generate realistic customer data for CSV file."""
    
    def __init__(self):
        self.first_names = [
            'James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda',
            'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
            'Thomas', 'Sarah', 'Christopher', 'Karen', 'Charles', 'Nancy', 'Daniel', 'Lisa',
            'Matthew', 'Betty', 'Anthony', 'Helen', 'Mark', 'Sandra', 'Donald', 'Donna'
        ]
        
        self.last_names = [
            'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
            'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
            'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
            'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker'
        ]
        
        self.domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'company.com']
        self.cities = [
            'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
            'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville',
            'Fort Worth', 'Columbus', 'Charlotte', 'San Francisco', 'Indianapolis', 'Seattle'
        ]
        self.states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'FL', 'OH', 'NC', 'WA', 'IN']
    
    def generate_customer_record(self, customer_id):
        """Generate a single customer record."""
        first_name = random.choice(self.first_names)
        last_name = random.choice(self.last_names)
        
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@{random.choice(self.domains)}"
        phone = f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"
        street_number = random.randint(1, 9999)
        street_name = f"{random.choice(self.last_names)} {random.choice(['St', 'Ave', 'Blvd', 'Dr', 'Ln'])}"
        address = f"{street_number} {street_name}"
        city = random.choice(self.cities)
        state = random.choice(self.states)
        zip_code = random.randint(10000, 99999)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=5*365)
        random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        registration_date = random_date.strftime('%Y-%m-%d')
        age = random.randint(18, 85)
        annual_income = random.randint(25000, 250000)
        credit_score = random.randint(300, 850)
        account_balance = round(random.uniform(0, 50000), 2)
        
        return [
            customer_id,
            first_name,
            last_name,
            email,
            phone,
            address,
            city,
            state,
            zip_code,
            age,
            annual_income,
            credit_score,
            account_balance,
            registration_date,
            random.choice(['Active', 'Inactive', 'Suspended']),
            random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
            ''.join(random.choices(string.ascii_letters + string.digits, k=20))  # Random notes
        ]

def generate_customer_batch(args):
    """Generate a batch of customer records. This function will be called by worker processes."""
    start_id, batch_size, seed = args
    
    # Set random seed for reproducible results in each process
    random.seed(seed)
    
    generator = CustomerDataGenerator()
    records = []
    
    for i in range(batch_size):
        customer_id = start_id + i
        record = generator.generate_customer_record(customer_id)
        records.append(record)
    
    return records

def write_chunk_to_temp_file(chunk_data, chunk_id, temp_dir):
    """Write a chunk of data to a temporary file."""
    temp_file_path = os.path.join(temp_dir, f"chunk_{chunk_id:06d}.csv")
    
    with open(temp_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(chunk_data)
    
    return temp_file_path, len(chunk_data)

def estimate_records_needed(target_size_bytes):
    """Estimate how many records we need to reach target size."""
    generator = CustomerDataGenerator()
    sample_records = [generator.generate_customer_record(i) for i in range(1000)]
    
    with tempfile.NamedTemporaryFile(mode='w', newline='', delete=False) as temp_file:
        writer = csv.writer(temp_file)
        headers = [
            'customer_id', 'first_name', 'last_name', 'email', 'phone',
            'address', 'city', 'state', 'zip_code', 'age', 'annual_income',
            'credit_score', 'account_balance', 'registration_date', 'status',
            'membership_tier', 'notes'
        ]
        writer.writerow(headers)
        writer.writerows(sample_records)
        temp_file.flush()
        sample_size = os.path.getsize(temp_file.name)
    
    os.unlink(temp_file.name)
    avg_bytes_per_record = (sample_size - len(','.join(headers).encode('utf-8'))) / len(sample_records)
    estimated_records = int(target_size_bytes / avg_bytes_per_record)
    
    logger.info(f"Estimated bytes per record: {avg_bytes_per_record:.2f}")
    logger.info(f"Estimated records needed: {estimated_records:,}")
    
    return estimated_records

def generate_csv_file_multiprocessing(file_path, target_size_bytes):
    """Generate a CSV file with customer data using multiprocessing."""
    logger.info(f"Starting multiprocessing CSV generation: {file_path}")
    logger.info(f"Target size: {target_size_bytes / (1024**3):.2f} GB")
    logger.info(f"Using {NUM_PROCESSES} processes")
    logger.info(f"Chunk size: {CHUNK_SIZE:,} records")
    
    start_time = time.time()
    
    estimated_records = estimate_records_needed(target_size_bytes)
    temp_dir = tempfile.mkdtemp(prefix='csv_generation_')
    
    try:
        work_chunks = []
        current_id = 1
        chunk_id = 0
        
        while current_id <= estimated_records:
            remaining_records = estimated_records - current_id + 1
            batch_size = min(CHUNK_SIZE, remaining_records)
            seed = hash((chunk_id, time.time())) % (2**32)
            work_chunks.append((current_id, batch_size, seed))
            
            current_id += batch_size
            chunk_id += 1
        
        logger.info(f"Created {len(work_chunks)} work chunks")
        
        chunk_files = []
        total_records = 0
        
        with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
            future_to_chunk = {}
            for i, chunk_args in enumerate(work_chunks):
                future = executor.submit(generate_customer_batch, chunk_args)
                future_to_chunk[future] = i
            
            for future in as_completed(future_to_chunk):
                chunk_idx = future_to_chunk[future]
                try:
                    chunk_data = future.result()
                    chunk_file, record_count = write_chunk_to_temp_file(
                        chunk_data, chunk_idx, temp_dir
                    )
                    chunk_files.append(chunk_file)
                    total_records += record_count
                    progress = (len(chunk_files) / len(work_chunks)) * 100
                    elapsed = time.time() - start_time
                    logger.info(f"Chunk progress: {progress:.1f}% | "
                               f"Chunks completed: {len(chunk_files)}/{len(work_chunks)} | "
                               f"Records: {total_records:,} | "
                               f"Time: {elapsed:.1f}s")
                    
                except Exception as e:
                    logger.error(f"Error processing chunk {chunk_idx}: {e}")
        
        logger.info("Combining chunks into final CSV file...")
        combine_start = time.time()
        
        headers = [
            'customer_id', 'first_name', 'last_name', 'email', 'phone',
            'address', 'city', 'state', 'zip_code', 'age', 'annual_income',
            'credit_score', 'account_balance', 'registration_date', 'status',
            'membership_tier', 'notes'
        ]
        chunk_files.sort()
        
        with open(file_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(headers)
            
            for i, chunk_file in enumerate(chunk_files):
                with open(chunk_file, 'r', encoding='utf-8') as infile:
                    reader = csv.reader(infile)
                    for row in reader:
                        writer.writerow(row)
                
                if (i + 1) % 10 == 0 or i == len(chunk_files) - 1:
                    combine_progress = ((i + 1) / len(chunk_files)) * 100
                    logger.info(f"Combining progress: {combine_progress:.1f}%")
        
        combine_time = time.time() - combine_start
        logger.info(f"Chunk combining completed in {combine_time:.1f} seconds")
        
    finally:
        logger.info("Cleaning up temporary files...")
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    final_size = os.path.getsize(file_path)
    final_size_gb = final_size / (1024**3)
    total_time = time.time() - start_time
    
    logger.info("="*60)
    logger.info("CSV GENERATION COMPLETED!")
    logger.info(f"Final size: {final_size_gb:.2f} GB")
    logger.info(f"Total records: {total_records:,}")
    logger.info(f"Total time: {total_time:.1f} seconds")
    logger.info(f"Average speed: {total_records/total_time:.0f} records/second")
    logger.info("="*60)
    
    return file_path, final_size

def generate_csv_file(file_path, target_size_bytes):
    """Generate a CSV file with customer data up to target size."""
    return generate_csv_file_multiprocessing(file_path, target_size_bytes)

def upload_to_sftp(local_file_path, remote_file_path, host, port, username, password):
    """Upload file to SFTP server."""
    logger.info(f"Starting SFTP upload to {host}:{port}")
    logger.info(f"Local file: {local_file_path}")
    logger.info(f"Remote path: {remote_file_path}")
    
    try:
        ssh_client = SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        logger.info("Connecting to SFTP server...")
        ssh_client.connect(hostname=host, port=port, username=username, password=password)
        sftp_client = ssh_client.open_sftp()
        file_size = os.path.getsize(local_file_path)
        file_size_gb = file_size / (1024**3)
        logger.info(f"File size: {file_size_gb:.2f} GB")
        start_time = time.time()
        
        def progress_callback(transferred, total):
            if total > 0:
                percent = (transferred / total) * 100
                elapsed = time.time() - start_time
                if elapsed > 0:
                    speed_mbps = (transferred / (1024**2)) / elapsed
                    logger.info(f"Upload progress: {percent:.1f}% | "
                               f"Speed: {speed_mbps:.1f} MB/s | "
                               f"Transferred: {transferred / (1024**3):.2f} GB")
        
        logger.info("Starting file upload...")
        sftp_client.put(local_file_path, remote_file_path, callback=progress_callback)
        
        upload_time = time.time() - start_time
        avg_speed_mbps = (file_size / (1024**2)) / upload_time if upload_time > 0 else 0
        
        logger.info("Upload completed successfully!")
        logger.info(f"Upload time: {upload_time:.1f} seconds")
        logger.info(f"Average speed: {avg_speed_mbps:.1f} MB/s")
        sftp_client.close()
        ssh_client.close()
        
        return True
        
    except Exception as e:
        logger.error(f"SFTP upload failed: {str(e)}")
        return False

def main():
    """Main function to generate CSV and upload to SFTP."""
    try:
        logger.info("="*60)
        logger.info("STARTING CSV GENERATION AND SFTP UPLOAD")
        logger.info(f"System info: {NUM_PROCESSES} CPU cores available")
        logger.info("="*60)
        
        file_path, file_size = generate_csv_file(LOCAL_FILE_PATH, TARGET_SIZE_BYTES)
        
        logger.info("="*60)
        logger.info("STARTING SFTP UPLOAD")
        logger.info("="*60)
        success = upload_to_sftp(
            local_file_path=LOCAL_FILE_PATH,
            remote_file_path=SFTP_REMOTE_PATH,
            host=SFTP_HOST,
            port=SFTP_PORT,
            username=SFTP_USERNAME,
            password=SFTP_PASSWORD
        )
        
        if success:
            logger.info("="*60)
            logger.info("PROCESS COMPLETED SUCCESSFULLY!")
            logger.info("="*60)
            cleanup = input("Remove local file after successful upload? (y/N): ")
            if cleanup.lower() == 'y':
                os.remove(LOCAL_FILE_PATH)
                logger.info(f"Local file {LOCAL_FILE_PATH} removed.")
        else:
            logger.error("Upload failed. Local file retained.")
            
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()