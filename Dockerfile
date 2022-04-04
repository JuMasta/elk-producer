FROM python:3.10.4-alpine3.15


WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT [ "python", "./start.py" ]
CMD ["period","=","60"]
