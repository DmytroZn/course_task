import os
from typing import Dict, Tuple
from multiprocessing import cpu_count

import requests
from urlextract import URLExtract

from homework2 import logger
from utils.utils import get_universal_pool, get_file_pickle


@logger.catch
def main(path_file: str):
    f = get_file_pickle(path_file)
    logger.info('Load file with text.')
    res_urls = get_list_of_urls(f)
    logger.debug(f'Got list of urls. {len(f)=}')
    res = get_universal_pool('Thread', len(res_urls), check_valid_url, None, res_urls, len(res_urls)*(3,))
    d1, d2 = {}, {}
    [(d1.update(i), d2.update(k) if k else None) for i, k in res]
    logger.debug(f'{len(d1)=} {len(d2)=}')
    logger.info(f'\n dict for first task: {d1} \n\n dict for second task: {d2} \n')
    return d1, d2


@logger.catch
def check_valid_url(origin_url: str, timeout: int = 2) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Check url is valid or fake

    :param origin_url: 'https://google.com'
    :param timeout: 5
    :return {'http://bit.ly/2G6QWX7': 200}, {'http://bit.ly/2G6QWX7': 'https://www.hcamag.com/ca/specialization/change-management/i-fell-in-love-with-hr/155318'}
    """
    new_url = None
    try:
        resp = requests.head(origin_url, timeout=timeout, allow_redirects=True)
        res_code = resp.status_code
        new_url = None if not (resp_history := resp.history) else resp_history[-1].headers['Location'] if resp_history[-1].status_code == 301 else None
    except requests.exceptions.RequestException as e:
        res_code = 'Not_find'
        logger.error(f'Somthing bad with check {origin_url=}. \n {e}')

    if new_url:
        return {origin_url: res_code}, {origin_url: new_url}

    return {origin_url: res_code}, {}


@logger.catch
def get_list_of_urls(texts: list) -> list:
    """Combine all results in single list

    :param texts: ['text text https://ebanoe.it and ane text https://google.com ',
                    'text text https://itea.ua this is text']
    :return: ['https://ebanoe.it', 'https://google.com', 'https://itea.ua']
    """

    my_res = list()
    cpu = 1 if (len_texts := len(texts)) < 100 else cpu_count()

    step = len_texts // cpu + 1
    res_text_cpu = [texts[i:i + step] for i in range(0, len_texts, step)]
    logger.info('Star extract url')
    res = get_universal_pool('Process', cpu, get_url_from_list_texts, tuple, res_text_cpu)
    logger.info('Finish extract url')
    [my_res.extend(i) for i in res]

    return my_res


@logger.catch
def get_url_from_list_texts(list_text: list) -> list:
    """This is function extract url link from list of texts

    :param list_text: ['text text https://ebanoe.it and ane text https://google.com ',
                    'text text https://itea.ua this is text']
    :return: ['https://ebanoe.it', 'https://google.com', 'https://itea.ua']
    """
    extractor = URLExtract()

    res_urls = list()
    urls_list = [[urls for urls in extractor.gen_urls(text)] for text in list_text]
    [res_urls.extend(i) for i in urls_list]

    return res_urls


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(__file__))
    file = os.path.join(base_dir, 'messages_to_parse.dat')
    logger.info('\n START PROGRAM')
    try:
        main(file)
    except Exception as e:
        logger.error(f'Something bad with program. \n {e}')
        logger.debug('FINISH PROGRAM WITH ERROR')
    logger.info('FINISH PROGRAM \n')



