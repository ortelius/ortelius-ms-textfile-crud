FROM python:3.8-slim-buster
COPY . /app
WORKDIR /app

RUN pip install psycopg2-binary
RUN pip3 install -r requirements.txt

EXPOSE 5000

ENTRYPOINT [ "python" ]
CMD [ "app.py" ]