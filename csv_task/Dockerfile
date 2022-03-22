FROM python:3.8.10
RUN mkdir /app
COPY ./requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
COPY . /app
CMD ["python", "/app/wsgi.py"]
#