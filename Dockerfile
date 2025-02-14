FROM ubuntu:18.04
ARG DEBIAN_FRONTEND=noninteractive

RUN  apt-get update \
  && apt-get install -y wget \
     gnupg2

RUN wget -qO - https://www.mongodb.org/static/pgp/server-4.2.asc | apt-key add -

RUN echo "deb [ arch=amd64,arm64 ] http://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.2 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-4.2.list

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    g++ \
    git \
    groff \
    jq \
    less \
    libpython3.8 \
    libpython3.8-dev \
    locales \
    locales-all \
    mongodb-org \
    npm \
    python3-dev \
    python3-pip \
    python3-setuptools \
    python3.8 \
    python3.8-dev \
    unzip \
    zip \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python3.8 get-pip.py

# Install version 16 of NodeJS
RUN curl -fsSL https://deb.nodesource.com/setup_16.x | bash -
RUN apt-get install -y nodejs

RUN apt-get install -y libpq-dev
RUN apt-get install -y gcc

WORKDIR /src

ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US.UTF-8

COPY requirements.txt /src/requirements.txt

RUN mkdir /src/tmp
RUN mkdir -p ./data/entries_software
RUN mkdir -p ./data/entries_dataset

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt --proxy=${HTTP_PROXY}
RUN npm install elasticdump -g

COPY . /src
