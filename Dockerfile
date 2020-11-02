FROM python:3.7-stretch

WORKDIR /code

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY mydocker/main/ .

CMD [ "python3", "./_main_.py" ]
