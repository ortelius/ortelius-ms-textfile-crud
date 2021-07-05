import os
import json

import psycopg2
from flask import Flask, request
from flask_restful import Api, Resource

# Initialize flask
app = Flask(__name__)
api = Api(app)

# Initialize database connection
db_host = os.getenv("DB_HOST", "localhost")
db_name = os.getenv("DB_NAME", "postgres")
db_user = os.getenv("DB_USER", "postgres")
db_pass = os.getenv("DB_PASS", "postgres")
db_port = os.getenv("DB_PORT", "5432")

conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_pass, port=db_port)

class ComponentTextfile(Resource):
    def post(cls):
        try: 
            input_data = request.form
            
            file = json.loads(input_data.get('file'))
            compid = input_data.get('compid')
            filetype = input_data.get('filetype')
            
            line_no = 1
            data_list = []
            for line in file:
                d = (compid, filetype, line_no, line)
                line_no += 1
                data_list.append(d)
    
            cursor = conn.cursor()
            #pre-processing
            pre_process = 'DELETE FROM dm.dm_textfile WHERE compid = %s AND filetype = %s;'
            cursor.execute(pre_process, [compid, filetype])
            
            if len(data_list) > 0:
                records_list_template = ','.join(['%s'] * len(data_list))
                sql = 'INSERT INTO dm.dm_textfile(compid, filetype, lineno, base64str) VALUES {}'.format(records_list_template)
                cursor.execute(sql, data_list)
    
            conn.commit()   # commit the changes
            cursor.close()
            
            return ({"message": f'components updated Succesfully'})
    
        except Exception as err:
            print(err)
            cursor = conn.cursor()
            cursor.execute("ROLLBACK")
            conn.commit()
            
            return ({"message": f'oops!, Something went wrong!'})
    
    def get(cls):
        try: 
            compid = request.args.get('compid')
            filetype = request.args.get('filetype')
            
            cursor = conn.cursor()
            sql = 'SELECT * FROM dm.dm_textfile WHERE compid = %s AND filetype = %s Order by lineno'
            cursor.execute(sql, [compid, filetype])
            records = cursor.fetchall()
            
            file = []
            for rec in records:
                file.append(rec[3])
                 
            # print (file) 
            conn.commit()   # commit the changes
            cursor.close()
            result = {"compid": compid, "filetype":filetype, "file": file}
            return ({"result": result, "message": f'components updated Succesfully'})
    
        except Exception as err:
            print(err)
            cursor = conn.cursor()
            cursor.execute("ROLLBACK")
            conn.commit()
            
            return ({"message": f'oops!, Something went wrong!'})
  
##
# Actually setup the Api resource routing here
##
api.add_resource(ComponentTextfile, '/msapi/textfile/')
  
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
