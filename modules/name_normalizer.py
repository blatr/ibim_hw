import asyncio
import json
import typing as tp
from itertools import chain

import aiohttp

EnhancedJsonEntry = tp.Dict[str, tp.Any]
EnhancedJsonType = tp.List[EnhancedJsonEntry]


class NameNormalizer:
    """
    Класс, реализующий нормализацию ФИО посредством обращения к API https://dadata.ru/clean/
    """

    def __init__(self, api_key: str = '123345ed9b47edc4f83f52e8b6b348057bff2f7b',
                 secret: str = '0a9e1944fc7a782b6efda5e506d937a3e69c70af') -> None:
        """
        Инициализурет объект класса.
        :param api_key: API ключ
        :type api_key: str
        :param secret: X-secret
        :type secret: str
        """
        self._api_key = api_key
        self._secret = secret
        self._path = 'https://cleaner.dadata.ru/api/v1/clean/name'
        self._chunk_size = 50

    async def multi_fetch_names(self, names: tp.List[str]) -> tp.List[tp.List[EnhancedJsonEntry]]:
        """
        Формирует пула батчей с ФИО для нормализации.(Размер батча продиктован требованиями со стороны API)
        :param names: набор ФИО
        :type names: list
        :return: обработанный набор ФИО
        :rtype: list
        """

        async def post_query(data: tp.List[str]) -> tp.List[str]:
            """
            Формирует post запрос к API.
            :param data: батч с ФИО
            :type data: list
            :return: обработанный батч с ФИО
            :rtype: list
            """
            headers = {
                'Authorization': f'Token {self._api_key}',
                'Content-Type': 'application/json',
                'X-Secret': f'{self._secret}'
            }
            async with session.post(self._path,
                                    data=json.dumps(data),
                                    headers=headers) as resp:
                return await resp.json()

        async with aiohttp.ClientSession() as session:
            return await asyncio.gather(
                *map(post_query, [names[idx * self._chunk_size:(idx + 1) * self._chunk_size]
                                  for idx in range(len(names) // self._chunk_size + 1)]))

    def normalize_names(self, df: EnhancedJsonType) -> EnhancedJsonType:
        """
        Нормализует набор ФИО.
        :param df: набор данных
        :type df: EnhancedJsonType
        :return: обработанныый набор данных
        :rtype: EnhancedJsonType
        """
        names = [obj['Name'] for obj in df]
        resulting_names = list(chain.from_iterable(asyncio.run(self.multi_fetch_names(names))))

        for idx, entry in enumerate(df):
            entry['Surname'], entry['Name'] = resulting_names[idx]['result'].split()
        return df
