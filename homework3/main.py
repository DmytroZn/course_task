import os

from homework1 import logger
from utils.utils import get_file_pickle, set_file_pickle, get_universal_pool, validate_num

from pympler import asizeof
from humanize import naturalsize
import pandas as pd
import numpy as np


@logger.catch
def load_csv_file(file_path: str, sep: str = ','):
    """
        Load file and dump to DataFrame

    :param file_path: '/home/dmytro/Documents/course_task/survey_results_public.csv'
    :param sep: ','
    :return: DataFrame
    """
    file_pickle = f"{os.path.splitext(file_path)[0]}.dat"
    if os.path.exists(file_pickle):
        df = get_file_pickle(file_pickle)
    elif os.path.exists(file_path):
        df = pd.read_csv(f, sep=sep)
        set_file_pickle(df, file_pickle)
    else:
        logger.error(f'Not find file in {file_path}')
        raise FileNotFoundError

    return df


@logger.catch
def dump_csv_zip_file(file_path: str, dataframe: pd.DataFrame, file_name: str, sep: str = ','):
    """
        Dump DataFrame to csv and zip

    :param file_path:
    :param sep:
    :return:
    """
    status = False
    file_path = os.path.dirname(file_path)
    file_name_csv = os.path.join(file_path, f"{file_name}.csv")
    file_name_zip = os.path.join(file_path, f"{file_name}.zip")
    compression_opts = dict(method='zip', archive_name=file_name_csv)

    try:
        dataframe.to_csv(path_or_buf=file_name_csv, sep=sep)
        dataframe.to_csv(path_or_buf=file_name_zip, index=False, compression=compression_opts)
        status = True
        logger.info(f'Created \n {file_name_csv=} \n and \n {file_name_zip=}')
    except:
        logger.error(f'Did not create \n {file_name_csv=} \n and \n {file_name_zip=}')

    return status


@logger.catch
def convert_df(dataframe, orient: str = None) -> tuple:
    """
        Convert DataFrame to data type in Python
        
    :param dataframe:
    :param orient:
    :return: (231812656, '231.8 MB', 'records')
    """
    if orient:
        dataframe = dataframe.to_dict(orient=orient)
    file_bytes = asizeof.asizeof(dataframe)
    file_size = naturalsize(file_bytes)

    return orient, file_bytes, file_size


@logger.catch
def validate_field_df(dataframe: pd.DataFrame):
    """
        Validate right field into DataFrame
    :param dataframe:
    :return:
    """
    error_msg = False
    fields = ('Country', 'Ethnicity', 'US_State', 'UK_Country', 'ResponseId', 'LearnCode', 'YearsCode', 'Age1stCode',
              'CompFreq', 'CompTotal', 'MainBranch', 'LanguageHaveWorkedWith')
    try:
        dataframe[[*fields]]
    except KeyError as e:
        col_names = dataframe.columns
        error_msg = f"\n\n Not enough field \n got {col_names} \n expect {fields} \n\n"
        logger.error(error_msg)

    return error_msg


@logger.catch
def main(file, file_name):
    """
    :param file: '/home/dmytro/Documents/course_task/survey_results_public.csv'
    :param file_name: 'filtered'
    :return:
    """
    logger.info('START PROGRAM')
    logger.info('Start load file')
    df = load_csv_file(file)
    logger.info('Finish load file')

    status_check = validate_field_df(df)
    if status_check:
        logger.debug('STOP PROGRAM NOT ENOUGH FIELD')
        raise KeyError(status_check)

    logger.info('Start get info statistic_memory')
    df_statistic_memory = get_statistic_memory(df)
    logger.info('Finish get info statistic_memory')

    # 2
    describe_df = df['Country'].describe()

    # 3
    count_nan_US_State = df[df['US_State'].isna()].describe().loc['count']['ResponseId']
    count_nan_UK_Country = df[df['UK_Country'].isna()].describe().loc['count']['ResponseId']

    # 4
    count_LearnCode = df[df['LearnCode'].isin(['School'])].count()['LearnCode']

    # 5
    mean_age_dev = pd.DataFrame(np.where(df['YearsCode'].notnull(), df['YearsCode'], False))[0].apply(validate_num)
    mean_age_dev = mean_age_dev[mean_age_dev != False].mean()

    # 6
    df_dev_ukr = df[(df['Age1stCode'] == '25 - 34 years') & (df['Country'] == 'Ukraine')]

    dev_ukr_count = df_dev_ukr.count()['ResponseId']
    df_dev_ukr_salary = df_dev_ukr[(df_dev_ukr['CompFreq'] == 'Monthly') & (df_dev_ukr['CompTotal'] < 100000)]['CompTotal'].mean()

    df_dev = pd.DataFrame(((df_dev_ukr_salary, dev_ukr_count),), columns=('df_dev_ukr_salary', 'dev_ukr_count'))
    # 7
    df.rename(columns={'MainBranch': 'BranchMain', 'LanguageHaveWorkedWith': 'ProSkills'}, inplace=True)

    # 8
    df.drop(['Ethnicity'], axis=1, inplace=True)

    # 9
    logger.info('Start dump files')
    status_dump = dump_csv_zip_file(file, df_dev, file_name)
    logger.info(f'Finish dump files with {status_dump=}')

    logger.info('FINISH PROGRAM')

    return (df_statistic_memory, describe_df, count_nan_US_State, count_nan_UK_Country, count_LearnCode, mean_age_dev,
            dev_ukr_count, df_dev_ukr_salary, status_dump)


@logger.catch
def get_statistic_memory(file):
    """
        Get statistic of convert to type

    :param file: DataFrame
    :return: DataFrame
    """
    orients = ('dict', 'list', 'split', 'records', None)
    len_params = len(orients)
    res = get_universal_pool('Process', len_params, convert_df, tuple, (file,)*len_params, orients)
    logger.debug(f"Get info memory \n {res=}")
    res = pd.DataFrame(data=res, columns=('orient', 'bytes', 'humanize')) if res else None
    return res


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(__file__))
    f = os.path.join(base_dir, 'survey_results_public.csv')
    main(f, 'filtered')

