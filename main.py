#!/usr/bin/env python

import uvicorn
from fastapi import FastAPI, Query, File, UploadFile, HTTPException
from fastapi import status
from fastapi.responses import FileResponse

from schemas import FileHash
from storage import Storage

api = FastAPI(debug=False, title='File storage HTTP API', version='0.1.0')

storage = Storage(dir_len=2)


async def is_hash(file_hash: str) -> bool:
    """
    Проверяет - является ли строка хэшом.
    Если не является - генерирует ошибку валидации.

    :param file_hash: предполагаемый хэш
    :return: является ли предполагаемый хэш хэшом.

    fixme: Вместо валидатора
    """

    try:
        return bool(int(file_hash, base=16))
    except ValueError:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"The string '{file_hash}' isn't a hash.")


@api.post(
    path='/upload/',
    response_model=FileHash,
    status_code=status.HTTP_201_CREATED,
    description='Loads the file. Returns the hash.',
    response_description='Hash of the uploaded file',
)
async def upload_file(
        file: UploadFile = File(..., description='File to upload.')) -> FileHash:
    """
    Загружает файл. Возвращает хэш.

    :param file: файл для загрузки
    :return: хэш загруженного файла
    """
    try:
        file_hash = await storage.upload_file(file=file)
    except ValueError:
        raise HTTPException(status_code=409, detail='File already exists.')
    except OSError:
       raise HTTPException(status_code=404, detail='Failed to upload file.')
    else:
        return FileHash(file_hash=file_hash)


@api.get(
    path='/download/{file_hash}',
    # response_model=FileResponse,
    description='Loads the file by hash.',
    response_description='File'
)
async def download_file(
        file_hash: str = Query(..., description='File hash to download.',
                               regex='[a-f0-9]')) -> FileResponse:
    """
    Зугружает файл по хэшу.

    :param file_hash: хэш файла для скачивания
    :return: адрес для загрузки файла
    """

    if await is_hash(file_hash):
        try:
            file_path = await storage.download_file(file_hash=file_hash)
        except FileNotFoundError:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail='File not exists.')
        except OSError:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail='Failed to upload file.')
        else:
            file_name = file_path.parts[-1]
            return FileResponse(path=file_path, status_code=200, filename=file_name)


@api.delete(
    path='/remove/{file_hash}',
    response_model=str,
    description='Removes the file by hash.',
    response_description='The confirmation',
)
async def remove_file(
        file_hash: str = Query(..., description='File hash to remove.',
                               regex='[a-f0-9]')) -> str:
    """
    Удаляет файл.

    :param file_hash: хэш файла для удаления
    :return: успешный ответ или ответ с ошибкой
    """

    if await is_hash(file_hash):
        try:
            await storage.remove_file(file_hash=file_hash)
        except FileNotFoundError:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail='File not exists.')
        except OSError:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail='Failed to remove file.')
        return 'Successful.'


if __name__ == '__main__':
    uvicorn.run(app=api)
