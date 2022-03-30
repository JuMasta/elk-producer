FROM python:3.10.4-alpine3.15


WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./elastic_search_error_logs_supplier.py" ]
