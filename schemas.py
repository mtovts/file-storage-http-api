from fastapi import Query
from pydantic import BaseModel, validator


class FileHash(BaseModel):
    file_hash: str = Query(..., description='Hash of the uploaded file.', regex='[a-f0-9]')

    # @validator('file_hash')
    # def is_hash(clf, v):
    #     int(v, base=16)
    #     return v
