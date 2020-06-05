import json
import os
import typing as tp
from operator import itemgetter

from pandas import json_normalize, ExcelWriter  # type: ignore

from modules import *

SOURCE_DATA_PATH = 'data/SourceData_JSON'
EnhancedJsonType = tp.List[tp.Dict[str, tp.Any]]


def read_source_data(source_name: str) -> EnhancedJsonType:
    """
    Вычитывает json файл из папки SOURCE_DATA_PATH.
    :type source_name: str
    :param source_name: название файла
    :return: json файл
    :rtype: EnhancedJsonType
    """
    with open(source_name, 'r') as f:
        return json.load(f)


def write_to_file(file, target_name: str, write_mode: str = 'a', to_excel=False, sheet_name: str = 'Лист1') -> None:
    """
    Записывает данные в текстовый\\xlsx файл.
    :param file: данные для записи
    :type file: list
    :param target_name: название файла
    :type target_name: str
    :param write_mode: тип записи в файл, optional
    :type write_mode: str
    :param to_excel: флаг записи в xlsx файл
    :type to_excel: bool
    :param sheet_name: название листа (используется только при записи в xlsx), optional
    :type sheet_name: str
    :return: None
    :rtype: None
    """
    if write_mode == 'a' and not os.path.exists(f'result/{target_name}.xlsx'):
        write_mode = 'w'

    if to_excel:
        with ExcelWriter(f'result/{target_name}.xlsx', mode=write_mode) as writer:
            normalized_json = json_normalize(file)
            normalized_json.to_excel(writer,
                                     sheet_name=sheet_name,
                                     index=False)
    else:
        with open(f'result/{target_name}', mode=write_mode) as writer:
            print(*file, sep='\n', file=writer)


def process_person_df(target: tp.Tuple[str, str], sorting_column: str) -> EnhancedJsonType:
    """
    Обрабатывает набор данных Person. (Все задания кроме 1.5, которое идет отдельно ниже)
    :param target: название файла с набором данных\\тип набора
    :type target: tuple
    :param sorting_column: колонка с типом сортировки по набору
    :type sorting_column: str
    :return: Обработанный json файл
    :rtype: EnhancedJsonType
    """
    name_norm = NameNormalizer()

    target_name, data_type = target
    person_df = parse_age(read_source_data(f'{SOURCE_DATA_PATH}/{target_name}.json'))

    mistyped_entries = find_mistyped_entries(person_df)
    write_to_file(file=mistyped_entries, target_name=f'1.7 {target_name}', write_mode='w')

    person_df = sorted(name_norm.normalize_names(person_df), key=itemgetter(sorting_column))
    write_to_file(file=person_df, target_name='1.4 resulting_data', write_mode='a', to_excel=True,
                  sheet_name=data_type)

    namesakes_df = find_namesakes_with_10_years_difference(person_df)
    write_to_file(file=namesakes_df, target_name=f'1.6 {target_name}', write_mode='w')
    return person_df


def process_contact_df(target: tp.Tuple[str, str], person_df: EnhancedJsonType) -> None:
    """
    Обрабатывает набор данных Contact.
    :param target: название файла с набором данных\\тип набора
    :type target: tuple
    :param person_df: обработанный набор данных Person
    :type person_df: EnhancedJsonType
    :return: None
    :rtype: None
    """
    target_name, data_type = target
    contact_df = parse_date(read_source_data(f'{SOURCE_DATA_PATH}/{target_name}.json'))
    contacts_counter = count_contacts_per_member(contact_df)
    write_to_file(reorder_df(person_df, contacts_counter, 'Contacts_Number'), target_name=f'2.4 {target_name}',
                  write_mode='w')

    duration_counter = find_total_duration_per_member(contact_df)
    write_to_file(reorder_df(person_df, duration_counter, 'Total_contacts_duration_in_seconds'),
                  target_name=f'2.5 {target_name}',
                  write_mode='w')

    id_to_age_map = cache_column_from_json(person_df, 'Age')
    most_talkative_age_group = count_contacts_per_age_group(contact_df, id_to_age_map)
    write_to_file(most_talkative_age_group, target_name=f'2.6 {target_name}',
                  write_mode='w')


if __name__ == '__main__':
    person_small = process_person_df(('small_data_persons', 'small_data'), 'Surname')
    person_big = process_person_df(('big_data_persons', 'big_data'), 'Name')

    lost_surnames = find_lost_surnames_in_bigdata(person_small, person_big)
    write_to_file(file=lost_surnames, target_name='1.5 lost_surnames', write_mode='w')

    process_contact_df(('small_data_contracts', 'small_data'), person_small)
    process_contact_df(('big_data_contracts', 'big_data'), person_big)
