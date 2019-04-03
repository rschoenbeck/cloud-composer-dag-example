FROM python:3.6.6-slim

COPY ./requirements.txt /
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir /files
COPY ./test_script.py /

ENTRYPOINT ["./python","test_script.py"]
