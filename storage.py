#!/usr/bin/env python

from hashlib import sha1
from pathlib import Path

from fastapi import UploadFile


class Storage:
    """Класс для работы с файловым хранилищем."""

    CHUNK_SIZE = 128 * 1024  # Размер части файла

    def __init__(self, path: Path = None, dir_len: int = 2, hash_func=None):
        """
        :param path: путь хранилища
        :param dir_len: длина подкаталога
        :param hash_func: алгоритм хэширования. По умолчанию = SHA-1
        """

        self.path = path or (Path.cwd() / Path('store'))
        self.dir_len = dir_len
        self.h = hash_func or sha1()

        if not self.path.exists():
            self.path.mkdir()

    async def get_file_hash(self, file: UploadFile) -> str:
        """
        Хэширует файл c помощью алгоритма хэширования. Возвращает хэш.
        Читает файл по частям и обновляет хэш.

        :param file: хэшируемый файл
        :return хэш файла, полученный с помощью алгоритма
        """

        h = self.h

        with open(file.filename, mode='rb', buffering=0) as f:
            for file_part in iter(lambda: f.read(self.CHUNK_SIZE), b''):
                h.update(file_part)
        return h.hexdigest()

    async def get_file_directory(self, file_hash: str) -> Path:
        """
        Возвращает директорию для файла по хэшу - первые dir_len символов хэша.

        :param file_hash: хэш файла
        :return: каталог для файла
        """
        return self.path / Path(file_hash[:self.dir_len])

    async def get_file_extension(self, file_hash: str) -> str:
        """
        Возвращает расширение файла с переданым хэшом.
        Если файла нет - генерирует исключение.

        :param file_hash: хэш файла, для которого требутся расширение
        :return: расширение файла
        """

        file_dir = await self.get_file_directory(file_hash=file_hash)
        path_gen = file_dir.iterdir()
        file_paths = [path for path in path_gen if path.is_file()]
        for file in file_paths:
            file_name_parts = file.parts[-1].rpartition('.')
            if file_name_parts[0] == file_hash:
                return file_name_parts[-1]

        raise FileNotFoundError(f"Файл с хэшом '{file_hash}' не найден.")

    async def upload_file(self, file: UploadFile) -> str:
        """
        Загружает файл под именем хэша.
        Загружает чайл по частям.

        :param file: файл для загрузки
        :return: хэш загруженного файла
        """

        file_hash = await self.get_file_hash(file)
        # todo: Добавить обработку файлов без расширения.
        #  Пока считаем, что у всех файлов есть расширение.
        file_extension = file.filename.rpartition('.')[-1]

        file_dir = await self.get_file_directory(file_hash=file_hash)
        file_path = file_dir / Path('.'.join((file_hash, file_extension)))

        if not file_dir.exists():
            file_dir.mkdir()
            print(f"Создан каталог '{file_dir}'.")
        elif file_path.exists():
            raise ValueError(f"Файл с хэшем '{file_hash}' уже загружен.")

        file.file.seek(0)  # возвращаемся в начало файла

        with open(file_path, mode='wb', buffering=0) as f:
            for chunk in iter(lambda: file.file.read(self.CHUNK_SIZE), b''):
                f.write(chunk)
        print(f"Загружен файл '{file.filename}'.")

        return file_hash

    async def download_file(self, file_hash: str) -> Path:
        """
        Отдает имя файла по хэшу.
        Если файла нет - генерирует исключение.

        :param file_hash: хэш запрашиваемого файла
        :return: имя файла
        """

        file_dir = await self.get_file_directory(file_hash=file_hash)
        file_extension = await self.get_file_extension(file_hash=file_hash)
        file_path = file_dir / Path('.'.join((file_hash, file_extension)))

        if not file_path.exists():
            raise FileNotFoundError(f"Файл '{file_path}' не найден.")
        return file_path

    async def remove_file(self, file_hash: str):
        """
        Удаляет файл по его хэшу.
        Если директория остается пустой - удаляет ее тоже.
        Если файла нет - генерирует исключение.

        :param file_hash: хэш файла, который нужно удалить
        """

        file_dir = await self.get_file_directory(file_hash=file_hash)
        file_extension = await self.get_file_extension(file_hash=file_hash)
        file_path = file_dir / Path('.'.join((file_hash, file_extension)))

        if file_path.exists() and file_path.is_file():
            file_path.unlink()
            print(f"Удален файл '{file_hash}.{file_extension}'.")

            if file_dir.is_dir() and not any(file_dir.iterdir()):
                file_dir.rmdir()
                print(f"Удален каталог '{file_dir}'.")
        else:
            raise FileNotFoundError(f"Файл '{file_path}' не найден.")
