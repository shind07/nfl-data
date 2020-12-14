FROM ubuntu:20.10

RUN apt-get update \
    && apt-get install -y  \
        python3 \ 
        python3-pip \
        git \
        postgresql-client \
    && apt-get clean

COPY requirements.txt tmp/requirements.txt
RUN pip3 install -r tmp/requirements.txt
RUN rm tmp/requirements.txt

WORKDIR /opt/nfl 
COPY app app
COPY alembic alembic

CMD [ "python3", "-m", "app" ]
