FROM python:3.8
WORKDIR /test
RUN pip install -r requirements.txt
COPY ./app ./app
CMD ["python", "./app/main.py"]
RUN chmod 777 /opt/airflow/*
