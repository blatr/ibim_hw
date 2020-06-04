import typing as tp
from collections import defaultdict, Counter
from copy import deepcopy
from datetime import datetime
from operator import itemgetter

EnhancedJsonType = tp.List[tp.Dict[str, tp.Union[str, int, datetime]]]


class SizableDict:
    """
    Класс, реализующий словарь множеств и подсчет размера каждого множества.
    """

    def __init__(self) -> None:
        """
        Инициализирует объект класса.
        """
        self.contacts = defaultdict(set)
        self.unique_contacts_per_member = Counter()

    def __setitem__(self, key: tp.Union[str, int], value: str) -> None:
        """
        Добавляет элемент в словарь.
        :param key: ключ в словаре
        :type key: str
        :param value: добавляемое значение
        :type value: str
        :return:None
        :rtype:None
        """
        if value not in self.contacts[key]:
            self.contacts[key].add(value)
            self.unique_contacts_per_member[key] += 1

    def __getitem__(self, item: tp.Union[str, int]) -> int:
        """
        Возвращает размер множества по ключу.
        :param item: ключ
        :type item: str,int
        :return: размер множества
        :rtype: int
        """
        return self.unique_contacts_per_member[item]

    @property
    def counts(self) -> tp.Dict[tp.Union[str, int], int]:
        """
        Возвращает словарь размеров множеств.
        :return: словарь
        :rtype: dict
        """
        return self.unique_contacts_per_member


def parse_date(df: EnhancedJsonType) -> EnhancedJsonType:
    """
    Конвертирует строку в дату в колонках From, To для каждого вхождения.
    :param df: набор данных
    :type df: EnhancedJsonType
    :return: обработанный набор данных
    :rtype: EnhancedJsonType
    """
    date_format = '%d.%m.%Y %H:%M:%S'
    for entry in df:
        entry['From'], entry['To'] = datetime.strptime(entry['From'], date_format), \
                                     datetime.strptime(entry['To'], date_format)
    return df


def count_contacts_per_member(df: EnhancedJsonType) -> tp.Dict[str, int]:
    """
    Считает количество уникальных контактов для каждого ID.
    :param df: набор данных
    :type df: EnhancedJsonType
    :return: словарь уникальных контактов
    :rtype: dict
    """
    member_contacts = SizableDict()
    for entry in df:
        if ((entry['To'] - entry['From']).total_seconds() / 60) >= 5:
            member_contacts[entry['Member1_ID']] = entry['Member2_ID']
            member_contacts[entry['Member2_ID']] = entry['Member1_ID']
    return member_contacts.counts


def count_contacts_per_age_group(df: EnhancedJsonType, age_dict: tp.Dict[str, int]) -> EnhancedJsonType:
    """
    Находит максимальное отношение количества уникальных контаков к количеству людей в разрере возрастной группы.
    :param df: набор данных
    :type df: EnhancedJsonType
    :param age_dict: словарь возрастов для каждого ID
    :type age_dict: dict
    :return: Возрастная группа с максимальным отношением
    :rtype: EnhancedJsonType
    """

    def update_max(potential_member: str) -> None:
        """
        Обновляет текущее максимальное значение, если это возможно.
        :param potential_member: ID человека
        :type potential_member: str
        :return: None
        :rtype: None
        """
        nonlocal max_contacts_per_age_group
        nonlocal max_age_group

        member_contacts_per_age_group = contacts_per_age_group[
                                            age_dict[potential_member]] / members_per_age_group[
                                            age_dict[potential_member]]
        if member_contacts_per_age_group > max_contacts_per_age_group:
            max_contacts_per_age_group = member_contacts_per_age_group
            max_age_group = age_dict[entry['Member1_ID']]

    members_per_age_group = SizableDict()
    contacts_per_age_group = SizableDict()

    max_contacts_per_age_group = 0
    max_age_group = 0

    for entry in df:
        if ((entry['To'] - entry['From']).total_seconds() / 60) >= 5:
            for member in [entry['Member1_ID'], entry['Member2_ID']]:
                members_per_age_group[age_dict[member]] = member

            contacts_per_age_group[age_dict[entry['Member1_ID']]] = entry['Member2_ID']
            contacts_per_age_group[age_dict[entry['Member2_ID']]] = entry['Member1_ID']

            for member in [entry['Member1_ID'], entry['Member2_ID']]:
                update_max(member)

    return [{'Age_group': max_age_group, 'Avg_contacts_per_age_group': max_contacts_per_age_group}]


def reorder_df(df: EnhancedJsonType, mask: tp.Dict[str, tp.Any], agg_column: str) -> EnhancedJsonType:
    """
    Добавляет новую колонку в набор данных и сортирует набор данных по этой колонке.
    :param df: набор данных
    :type df: EnhancedJsonType
    :param mask: маска для сортировки
    :type mask: dict
    :param agg_column: новая колонка
    :type agg_column: str
    :return: отсортированный набор данных
    :rtype: EnhancedJsonType
    """
    df_new = deepcopy(df)
    for entry in df_new:
        entry[agg_column] = mask[entry['ID']]
    return sorted(df_new, key=itemgetter(agg_column), reverse=True)


def find_total_duration_per_member(df: EnhancedJsonType) -> tp.Dict[str, int]:
    """
    Подсчитывает общую длительность контакта с другими людьми для каждого ID.
    :param df: набор данных
    :type df: EnhancedJsonType
    :return: словарь длительностей контактов
    :rtype: dict
    """
    duration_dict = Counter()
    for entry in df:
        contact_duration = (entry['To'] - entry['From']).total_seconds()
        for member in [entry['Member1_ID'], entry['Member2_ID']]:
            duration_dict[member] += contact_duration
    return duration_dict
