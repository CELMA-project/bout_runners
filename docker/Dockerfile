# Building bout_runners
# docker build -f Dockerfile -t bout_runners:latest .

# Use bout_dev built from
# https://github.com/CELMA-project/bout_install
FROM loeiten/bout_dev:latest

# Install dependencies
RUN  wget https://raw.githubusercontent.com/CELMA-project/bout_runners/master/requirements.txt \
    # NOTE: cython is needed to build numpy
    && pip3 install --no-cache-dir cython \
    # NOTE: Needed for building psutil
    # https://github.com/giampaolo/psutil/issues/664
    && apk add --update gcc libc-dev linux-headers
    && rm -rf /var/cache/apk/*
    # NOTE: pip install pandas fails before installing numpy as
    #       alpine does not support PEP 513
    # https://github.com/docker-library/python/issues/381
    && pip3 install --no-cache-dir numpy \
    && pip3 install --no-cache-dir -r requirements.txt \
    && rm requirements.txt

ENTRYPOINT ["/bin/sh"]