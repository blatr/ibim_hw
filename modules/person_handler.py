import re
import typing as tp
from collections import defaultdict
from itertools import groupby
from operator import itemgetter

EnhancedJsonType = tp.List[tp.Dict[str, tp.Any]]


def parse_age(df: EnhancedJsonType) -> EnhancedJsonType:
    """
    Конвертирует поле Age в тип int.
    :param df: набор данных
    :type df: EnhancedJsonType
    :return: обработанный набор данных
    :rtype: EnhancedJsonType
    """
    for entry in df:
        entry['Age'] = int(entry['Age'])
    return df


def cache_column_from_json(df: EnhancedJsonType, column_name: str) -> tp.Dict[str, int]:
    """
    Сохраняет значение колонки в словарь для каждого ID.
    :param df: набор данных
    :type df: EnhancedJsonType
    :param column_name: кэшируемая колонка
    :type column_name: str
    :return: словарь со значениями колонки
    :rtype: dict
    """
    id_to_age_map: tp.Dict[str, int] = dict()
    for entry in df:
        id_to_age_map[entry['ID']] = entry[column_name]
    return id_to_age_map


def find_mistyped_entries(df: EnhancedJsonType) -> EnhancedJsonType:
    """
    Находит записи в наборе данных, в которых присутствуют латинские буквы и заменяет их на кириллицу.
    :param df: набор данных
    :type df: EnhancedJsonType
    :return: Записи, в которых встретились латинские буквы.
    :rtype: EnhancedJsonType
    """
    mistyped_names = []
    eng_to_ru_table = {'a': 'а', 'e': 'е', 'o': 'о'}

    for entry in df:
        mistyped_letter = set(re.findall('[a-zA-Z]', entry['Name']))
        if mistyped_letter:
            for letter in mistyped_letter:
                entry['Name'] = entry['Name'].replace(letter, eng_to_ru_table[letter])
            mistyped_names.append(entry)

    return mistyped_names


def find_lost_surnames_in_bigdata(small_df: EnhancedJsonType, big_df: EnhancedJsonType) -> EnhancedJsonType:
    """
    Отбирает из маленького набора данных те фамилии, которых нет в большом наборе.
    :param small_df: маленький набор данных
    :type small_df: EnhancedJsonType
    :param big_df: большой набор данных
    :type big_df: EnhancedJsonType
    :return: Отфильтрованный набор данных
    :rtype: EnhancedJsonType
    """
    big_df_surnames = set([entry['Surname'] for entry in big_df])
    return [entry for entry in small_df if entry['Surname'] not in big_df_surnames]


def find_namesakes_with_10_years_difference(df: EnhancedJsonType) -> \
        tp.List[tp.Tuple[tp.Dict[str, tp.Any], tp.Dict[str, tp.Any]]]:
    """
    Находит пары однофамильцев с разницей в возрасте 10 лет.
    :param df: набор данных
    :type df: EnhancedJsonType
    :return: пары однофамильцев
    :rtype: list
    """
    df = sorted(df, key=itemgetter('Surname', 'Age'))
    namesakes_pairs = []

    for key, group in groupby(df, key=itemgetter('Surname')):
        materialized_group = list(group)
        seen_ages: tp.Dict[str, tp.List[int]] = defaultdict(list)

        for group_idx, entry in enumerate(materialized_group):
            for potential_namesake in [entry['Age'] - 10, entry['Age'] + 10]:
                for idx in seen_ages[potential_namesake]:
                    namesakes_pairs.append((entry, materialized_group[idx]))

            seen_ages[entry['Age']].append(group_idx)
    return namesakes_pairs
