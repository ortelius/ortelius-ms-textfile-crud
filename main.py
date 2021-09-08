import os
import json
import yaml
import base64
import requests
import psycopg2
from sqlalchemy import create_engine
from fastapi import FastAPI, Query, Request, Response, HTTPException, responses, status
from pydantic import BaseModel, Field
from typing import List, Optional

# Init Globals
service_name = 'ortelius-ms-textfile-crud'

# Init FastAPI
app = FastAPI(
    title="ortelius-ms-textfile-crud",
    description="TextFile Crud APIs",
    version="1.0.0",
    terms_of_service="http://swagger.io/terms/",
    contact={
        "email": "xyz@deployhub.com",
    },
    license_info={
        "name": "Apache 2.0",
        "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
    },)

# Init db connection
db_host = os.getenv("DB_HOST", "localhost")
db_name = os.getenv("DB_NAME", "postgres")
db_user = os.getenv("DB_USER", "postgres")
db_pass = os.getenv("DB_PASS", "postgres")
db_port = os.getenv("DB_PORT", "5432")

validateuser_url = os.getenv("VALIDATEUSER_URL", "http://localhost:5000")

engine = create_engine("postgresql+psycopg2://" + db_user + ":" + db_pass + "@" + db_host + "/" + db_name)


class StatusMsg(BaseModel):
    status: str
    service_name: Optional[str] = None


@app.get("/health",
         responses={
             503: {"model": StatusMsg,
                   "description": "DOWN Status for the Service",
                   "content": {
                       "application/json": {
                           "example": {"status": 'DOWN'}
                       },
                   },
                   },
             200: {"model": StatusMsg,
                   "description": "UP Status for the Service",
                   "content": {
                       "application/json": {
                           "example": {"status": 'UP', "service_name": service_name}
                       }
                   },
                   },
         }
         )
async def health(response: Response):
    try:
        with engine.connect() as connection:
            conn = connection.connection
            cursor = conn.cursor()
            cursor.execute('SELECT 1')
            if cursor.rowcount > 0:
                return {"status": 'UP', "service_name": service_name}
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {"status": 'DOWN'}
    except Exception as err:
        print(str(err))
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": 'DOWN'}


def get_mimetype(filetype, dstr):
    if (filetype.lower() == 'readme'):
        return 'text/markdown'
    try:
        json.loads(dstr)
        return 'application/json'
    except:
        pass

    try:
        yaml.safe_load(dstr)
        return 'text/yaml'
    except:
        pass

    return 'text/plain'


class Message(BaseModel):
    detail: str


@app.get('/msapi/textfile/',
         responses={
             401: {"model": Message,
                   "description": "Authorization Status",
                   "content": {
                       "application/json": {
                           "example": {"detail": "Authorization failed"}
                       },
                   },
                   },
             500: {"model": Message,
                   "description": "SQL Error",
                   "content": {
                       "application/json": {
                           "example": {"detail": "SQL Error: 30x"}
                       },
                   },
                   },
             200: {"description": "File Content"},
         }
         )
async def getFileContent(request: Request, response: Response, compid: int = Query(..., ge=1), filetype: str = Query(..., regex="^(?!\s*$).+")):
    try:
        result = requests.get(validateuser_url + "/msapi/validateuser", cookies=request.cookies)
        if (result is None):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization Failed")

        if (result.status_code != status.HTTP_200_OK):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization Failed status_code=" + str(result.status_code))
    except Exception as err:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization Failed:" + str(err)) from None

    try:
        with engine.connect() as connection:
            conn = connection.connection

            if (filetype is None and 'swagger' in request.path_params):
                filetype = 'swagger'

            cursor = conn.cursor()
            sql = 'SELECT * FROM dm.dm_textfile WHERE compid = %s AND filetype = %s Order by lineno'
            cursor.execute(sql, [compid, filetype])
            records = cursor.fetchall()
            cursor.close()
            conn.commit()

            file = []
            for rec in records:
                file.append(rec[3])

            encoded_str = "".join(file)
            decoded_str = base64.b64decode(encoded_str).decode("utf-8")
            response.headers['Content-Type'] = get_mimetype(filetype, decoded_str) + '; charset=utf-8'
            return decoded_str

    except HTTPException:
        raise
    except Exception as err:
        print(str(err))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(err)) from None


class FileRequest(BaseModel):
    compid: int = Field(..., ge=1)
    filetype: str = Field(..., regex="^(?!\s*$).+")
    file: List[str]


@app.post('/msapi/textfile/',
          responses={
              401: {"model": Message,
                    "description": "Authorization Status",
                    "content": {
                        "application/json": {
                            "example": {"detail": "Authorization failed"}
                        },
                    },
                    },
              500: {"model": Message,
                    "description": "SQL Error",
                    "content": {
                        "application/json": {
                            "example": {"detail": "SQL Error: 30x"}
                        },
                    },
                    },
              200: {"model": Message,
                    "description": "Components Updated",
                    "content": {
                        "application/json": {
                            "example": {"detail": "components updated succesfully"}
                        },
                    },
                    },
          }
          )
async def saveFileContent(fileRequest: FileRequest):
    try:
        result = requests.get(validateuser_url + "/msapi/validateuser", cookies=request.cookies)
        if (result is None):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization Failed")

        if (result.status_code != status.HTTP_200_OK):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization Failed status_code=" + str(result.status_code))
    except Exception as err:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization Failed:" + str(err)) from None

    try:
        with engine.connect() as connection:
            conn = connection.connection

            line_no = 1
            data_list = []
            for line in fileRequest.file:
                d = (fileRequest.compid, fileRequest.filetype, line_no, line)
                line_no += 1
                data_list.append(d)

            cursor = conn.cursor()
            # pre-processing
            pre_process = 'DELETE FROM dm.dm_textfile WHERE compid = %s AND filetype = %s;'
            cursor.execute(pre_process, [fileRequest.compid, fileRequest.filetype])

            if len(data_list) > 0:
                records_list_template = ','.join(['%s'] * len(data_list))
                sql = 'INSERT INTO dm.dm_textfile(compid, filetype, lineno, base64str) VALUES {}'.format(records_list_template)
                cursor.execute(sql, data_list)

            cursor.close()
            conn.commit()

            return Message(detail='components updated succesfully')

    except HTTPException:
        raise
    except Exception as err:
        print(str(err))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(err)) from None
