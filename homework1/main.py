import hashlib
import os
import shutil
import time

from homework1 import logger


@logger.catch
def main(path_files: str, path_copied_files: str):
    """
    :param path_files: '/home/dmytro/Documents/course_task/files'  folder where are files
    :param path_copied_files: '/home/dmytro/Documents/course_task/copied_files' folder where will have been copying files
    :return:
    """
    logger.info(f"Take names from: {path_files}")
    files_name = os.listdir(path_files)

    logger.info(f"Chack exist folder or not")
    if not os.path.exists(path_copied_files):
        os.mkdir(path_copied_files)
    else:
        shutil.rmtree(path_copied_files)
        os.mkdir(path_copied_files)

    copy_files(files_name, path_files, path_copied_files)


@logger.catch
def copy_files(files_name: list, path_files: str, path_copied_files: str):
    """
    :param files_name: ['1037586316', '1037502781', '1037593164', '1037538079', '1037559424']
    :param path_files: '/home/dmytro/Documents/course_task/files'  folder where are files
    :param path_copied_files: '/home/dmytro/Documents/course_task/copied_files' folder where will have been copying files

    :return:
    """
    data_files = set()
    logger.info("Start coping")
    for fl in files_name:
        file_name = os.path.join(path_files, fl)
        with open(file_name, 'rb') as f:
            bytes = f.read()
            hash_file = hashlib.sha256(bytes).hexdigest()
            try:
                if hash_file not in data_files:
                    time.sleep(2)
                    shutil.copy(file_name, os.path.join(path_copied_files, fl))
                    data_files.add(hash_file)
                    logger.debug(f"File {fl} with hash: {hash_file} hash_file, was copied")
                else:
                    logger.debug(f"File {fl} with hash: {hash_file} hash_file, exist")

            except Exception as e:
                logger.error(f"File {fl} with hash: {hash_file} hash_file, WAS NOT copied. \n {e}")

    logger.info("Finish coping")


if __name__ == "__main__":
    logger.info("START PROGRAMM")
    bese_dir = os.path.dirname(os.path.dirname(__file__))
    path_files = os.path.join(bese_dir, 'files')
    path_copied_files = os.path.join(bese_dir, 'copied_files')
    start_time = time.time()
    main(path_files, path_copied_files)
    end_time = time.time() - start_time
    logger.info(f"FINISH PROGRAMM, it`s take {end_time}")

