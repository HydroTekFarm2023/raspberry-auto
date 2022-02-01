FROM --platform=linux/amd64 python:3.7-stretch

WORKDIR /code

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY . .

CMD [ "python3", "-u", "./_main_.py" ]
