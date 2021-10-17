from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Union
import pickle

from homework2 import logger

from tqdm import tqdm


@logger.catch
def set_file_pickle(data, file_name: str) -> None:
    """
        This method dump pickle
    :param data:
    :param file_name:
    :return:
    """
    with open(file_name, 'wb') as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)


@logger.catch
def get_file_pickle(file_name: str):
    """
        This method load pickle
    :param file_name:
    :return:
    """
    with open(file_name, 'rb') as f:
        return pickle.load(f)


@logger.catch
def get_universal_pool(pool: str, max_workers: int, func, type_convert: Union[tuple, list, dict, None] = None,
                       *args, **kwargs) -> Union[tuple, list, dict]:
    """
        This method make or Thread Process

    :param pool: Thread | Process
    :param max_workers: 4
    :param func: my_func
    :param type_convert: dict | tuple | list
    :param *args: param for func
    :param **kwargs: param for func
    :return:
    """
    res = None
    if pool == 'Process':
        res = universal_executor(ProcessPoolExecutor, max_workers, func, type_convert, *args, **kwargs)
    else:
        res = universal_executor(ThreadPoolExecutor, max_workers, func, type_convert, *args, **kwargs)
    return res


def universal_executor(FuncdPoolExecutor, max_workers, func, type_convert, *args, **kwargs):
    res = None
    with FuncdPoolExecutor(max_workers=max_workers) as executor:
        try:
            res = tqdm(executor.map(func, *args, **kwargs), total=max_workers)
            res = type_convert(res) if type_convert else res
        except TypeError as e:
            logger.debug('You need write Sequence')
            logger.debug(f'{args=}\n, {kwargs=}\n')
            raise e
    return res


@logger.catch
def validate_num(n):
    try:
        n = int(n) if n else n
    except ValueError:
        n = False
    return n
