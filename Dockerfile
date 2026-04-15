ARG PG_MAJOR=18
FROM postgres:${PG_MAJOR}

ARG PG_MAJOR=18

RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-${PG_MAJOR} \
    libcurl4-openssl-dev \
    librdkafka-dev \
    libhiredis-dev \
    libmosquitto-dev \
    librabbitmq-dev \
    libnats-dev \
    libssl-dev \
    libipc-run-perl \
    redis-tools \
    kafkacat \
    wget \
    unzip \
    doxygen \
    clang-format \
    cppcheck \
    clang-tidy \
    bear \
    && rm -rf /var/lib/apt/lists/*
