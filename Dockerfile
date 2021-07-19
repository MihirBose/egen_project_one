FROM python:3.7.9

ADD stream_reader.py requirements.txt /

RUN pip install -r requirements.txt

CMD python stream_reader.py