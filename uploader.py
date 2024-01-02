import time
import sys
import os
import threading
from datetime import datetime

import boto3
from boto3.s3.transfer import TransferConfig

# Create log directory if it does not exist
if not os.path.exists("./uploader_logs"):
    os.makedirs("./uploader_logs")

# Redirect stdout to log file
sys.stdout = open(f"./uploader_logs/log_{datetime.now()}.txt", "w")

MB = 1024 * 1024
s3 = boto3.resource("s3")


class TransferCallback:
    """
    Handle callbacks from the transfer manager.

    The transfer manager periodically calls the __call__ method throughout
    the upload and download process so that it can take action, such as
    displaying progress to the user and collecting data about the transfer.
    """

    def __init__(self, target_size):
        self._target_size = target_size
        self._total_transferred = 0
        self._lock = threading.Lock()
        self.thread_info = {}

    def __call__(self, bytes_transferred):
        """
        The callback method that is called by the transfer manager.

        Display progress during file transfer and collect per-thread transfer
        data. This method can be called by multiple threads, so shared instance
        data is protected by a thread lock.
        """
        time.sleep(1)

        thread = threading.current_thread()
        with self._lock:
            self._total_transferred += bytes_transferred
            if thread.ident not in self.thread_info.keys():
                self.thread_info[thread.ident] = bytes_transferred
            else:
                self.thread_info[thread.ident] += bytes_transferred

            target = self._target_size * MB
            sys.stdout.write(
                f"\r{self._total_transferred} of {target} transferred "
                f"({(self._total_transferred / target) * 100:.2f}%)."
            )
            sys.stdout.flush()

def upload_with_chunksize_and_meta(
    local_file_path, bucket_name, object_key, file_size_mb, metadata=None
):
    """
    Upload a file from a local folder to an Amazon S3 bucket, setting a
    multipart chunk size and adding metadata to the Amazon S3 object.

    The multipart chunk size controls the size of the chunks of data that are
    sent in the request. A smaller chunk size typically results in the transfer
    manager using more threads for the upload.

    The metadata is a set of key-value pairs that are stored with the object
    in Amazon S3.
    """
    transfer_callback = TransferCallback(file_size_mb)

    config = TransferConfig(multipart_chunksize=5)
    extra_args = {"Metadata": metadata} if metadata else None
    s3.Bucket(bucket_name).upload_file(
        local_file_path,
        object_key,
        Config=config,
        ExtraArgs=extra_args,
        Callback=transfer_callback,
    )
    return transfer_callback.thread_info


bucket_name = ""


def check_for_stop():
    # Check if stop file exists
    with open("./backup_uploader_stopper.txt", "r") as f:
        if f.readline() == "true":
            return True
        else:
            return False

# Iterate through all files that should be uploaded on directory
def scan_dir_for_files(dir_path):
    print(f"Scanning directory {dir_path}")
    for file_name in os.listdir(dir_path):
        time.sleep(2)

        # Check if stop file indicates that uploader should stop
        if check_for_stop():
            print("Stopping uploader")
            return

        # Skip .DS_Store files
        if file_name.endswith(".DS_Store"):
            continue

        # Updating to current directory path
        file_path = dir_path + "/" + file_name

        # Recursively scan subdirectories
        if os.path.isdir(file_path):
            scan_dir_for_files(file_path)

        # Upload file if it is a zip file
        if file_path.endswith(".zip"):
            print(f"{datetime.now()} - Uploading {file_path} to Amazon S3 bucket {bucket_name}")
            file_stats = os.stat(file_path)
            file_size_in_mb = file_stats.st_size / (1024 * 1024)
            print(f'File Size in MegaBytes is {file_size_in_mb}')
            # upload_with_chunksize_and_meta(file_path, bucket_name, file_path, file_size_in_mb)

scan_dir_for_files("example_dir")

sys.stdout.close()
