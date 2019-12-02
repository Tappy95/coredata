From python:3.7

COPY . /app

WORKDIR /app

RUN pip install -p Pipfile \
    && groupadd -r admin && useradd -r -g admin admin

USER admin

ENV ENV_TYPE=PRODUCTION

ENTRYPOINT [ "python", "server.py"]
