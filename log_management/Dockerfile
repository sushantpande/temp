FROM python:2.7

RUN mkdir /opt/log_management

RUN mkdir /var/log/log_management

COPY . /opt/log_management/

WORKDIR /opt/log_management

RUN pip install -r requirements.txt

CMD  ["python", "app.py"]
