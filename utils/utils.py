from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Union
import pickle

from homework2 import logger


@logger.catch
def set_file_piclke(data, file_name: str) -> None:
    """
        This method dump pickle
    :param data:
    :param file_name:
    :return:
    """
    with open(file_name, 'wb') as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)


@logger.catch
def get_file_piclke(file_name: str):
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
    if pool == 'Thread':
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            try:
                res = executor.map(func, *args, **kwargs)
                res = type_convert(res) if type_convert else res
            except TypeError as e:
                logger.debug('You need write Sequence')
                logger.debug(f'{args=}\n, {kwargs=}\n')
                raise e
    elif pool == 'Process':
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            try:
                res = executor.map(func, *args, **kwargs)
                res = type_convert(res) if type_convert else res
            except TypeError as e:
                logger.debug('You need write Sequence')
                logger.debug(f'{args=}\n, {kwargs=}\n')
                raise e

    return res
