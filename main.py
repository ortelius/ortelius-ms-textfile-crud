import base64
import json
import os
from http import HTTPStatus

import psycopg2
import pybreaker
import requests
import sqlalchemy.pool as pool
import yaml
from flask import Flask, make_response, request
from flask_restful import Api, Resource
from flask_swagger_ui import get_swaggerui_blueprint
from webargs import fields, validate
from webargs.flaskparser import abort, parser


@parser.error_handler
def handle_request_parsing_error(err, req, schema, *, error_status_code, error_headers):
    print("inside error handler", err.messages)
    abort(HTTPStatus.BAD_REQUEST, errors=err.messages)


# Initialize flask
app = Flask(__name__)
api = Api(app)
app.url_map.strict_slashes = False

### swagger specific ###
SWAGGER_URL = '/swagger'
API_URL = '/static/swagger-ui/doc/swagger.yml'
SWAGGERUI_BLUEPRINT = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "ortelius-ms-textfile-crud"
    }
)
app.register_blueprint(SWAGGERUI_BLUEPRINT, url_prefix=SWAGGER_URL)
### end swagger specific ###

# Initialize database connection
db_host = os.getenv("DB_HOST", "localhost")
db_name = os.getenv("DB_NAME", "postgres")
db_user = os.getenv("DB_USER", "postgres")
db_pass = os.getenv("DB_PASS", "postgres")
db_port = os.getenv("DB_PORT", "5432")
validateuser_url = os.getenv("VALIDATEUSER_URL", "http://localhost:5000")

# connection pool config
conn_pool_size = int(os.getenv("POOL_SIZE", "3"))
conn_pool_max_overflow = int(os.getenv("POOL_MAX_OVERFLOW", "2"))
conn_pool_timeout = float(os.getenv("POOL_TIMEOUT", "30.0"))

conn_circuit_breaker = pybreaker.CircuitBreaker(
    fail_max=1,
    reset_timeout=10,
)


@conn_circuit_breaker
def create_conn():
    conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_pass, port=db_port)
    return conn


# connection pool init
mypool = pool.QueuePool(create_conn, max_overflow=conn_pool_max_overflow, pool_size=conn_pool_size, timeout=conn_pool_timeout)

# health check endpoint


class HealthCheck(Resource):
    def get(self):
        try:
            conn = mypool.connect()
            cursor = conn.cursor()
            cursor.execute('SELECT 1')
            conn.close()
            if cursor.rowcount > 0:
                return ({"status": 'UP', "service_name": 'ortelius-ms-textfile-crud'}), HTTPStatus.OK
            return ({"status": 'DOWN'}), HTTPStatus.SERVICE_UNAVAILABLE

        except Exception as err:
            print(err)
            return ({"status": 'DOWN'}), HTTPStatus.SERVICE_UNAVAILABLE


api.add_resource(HealthCheck, '/health')


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


class ComponentTextfile(Resource):
    def post(self):
        result = requests.get(validateuser_url + "/msapi/validateuser", cookies=request.cookies)
        if (result is None):
            return None, HTTPStatus.UNAUTHORIZED

        if (result.status_code != HTTPStatus.OK):
            return result.json(), HTTPStatus.UNAUTHORIZED

        request_validations = {
            "file": fields.Str(required=True),
            "compid": fields.Int(required=True, validate=validate.Range(min=1)),
            "filetype": fields.Str(required=True, validate=validate.Length(min=1))
        }

        parser.parse(request_validations, request, location="json")

        conn = None
        try:
            input_data = request.get_json()

            file = input_data.get('file', '')
            compid = input_data.get('compid', -1)
            filetype = input_data.get('filetype', '')

            line_no = 1
            data_list = []
            for line in file:
                d = (compid, filetype, line_no, line)
                line_no += 1
                data_list.append(d)

            conn = mypool.connect()
            cursor = conn.cursor()
            # pre-processing
            pre_process = 'DELETE FROM dm.dm_textfile WHERE compid = %s AND filetype = %s;'
            cursor.execute(pre_process, [compid, filetype])

            if len(data_list) > 0:
                records_list_template = ','.join(['%s'] * len(data_list))
                sql = 'INSERT INTO dm.dm_textfile(compid, filetype, lineno, base64str) VALUES {}'.format(records_list_template)
                cursor.execute(sql, data_list)

            conn.commit()   # commit the changes
            cursor.close()

            return ({"message": f'components updated succesfully'}), HTTPStatus.OK

        except Exception as err:
            print(err)
            if(conn is not None):
                conn.rollback()
            return ({"message": str(err)}), HTTPStatus.INTERNAL_SERVER_ERROR

    def get(self):
        result = requests.get(validateuser_url + "/msapi/validateuser", cookies=request.cookies)
        if (result is None):
            return None, HTTPStatus.UNAUTHORIZED

        if (result.status_code != HTTPStatus.OK):
            return result.json(), HTTPStatus.UNAUTHORIZED

        query_args_validations = {
            "compid": fields.Int(required=True, validate=validate.Range(min=1)),
            "filetype": fields.Str(required=True, validate=validate.Length(min=1))
        }

        parser.parse(query_args_validations, request, location="query")

        conn = None
        try:
            compid = request.args.get('compid')
            filetype = request.args.get('filetype', None)

            if (filetype is None and 'swagger' in request.path):
                filetype = 'swagger'

            conn = mypool.connect()
            cursor = conn.cursor()
            sql = 'SELECT * FROM dm.dm_textfile WHERE compid = %s AND filetype = %s Order by lineno'
            cursor.execute(sql, [compid, filetype])
            records = cursor.fetchall()

            file = []
            for rec in records:
                file.append(rec[3])

            conn.commit()   # commit the changes
            cursor.close()
            encoded_str = "".join(file)
            decoded_str = base64.b64decode(encoded_str).decode("utf-8")
            response = make_response(decoded_str)
            response.headers['Content-Type'] = get_mimetype(filetype, decoded_str) + '; charset=utf-8'
            return response

        except Exception as err:
            print(err)
            if(conn is not None):
                conn.rollback()
            return ({"message": str(err)}), HTTPStatus.INTERNAL_SERVER_ERROR


##
# Actually setup the Api resource routing here
##
api.add_resource(ComponentTextfile, '/msapi/textfile/')

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5002, debug=True)
