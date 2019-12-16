# Building bout_runners
# docker build -f Dockerfile -t bout_runners:latest .

# Use bout_dev built from
# https://github.com/CELMA-project/bout_install
FROM loeiten/bout_dev:latest

# Install dependencies
# FIXME: We install from the requirements file until we make a package
RUN  wget https://raw.githubusercontent.com/CELMA-project/bout_runners/master/requirements.txt \
    # Build suddenly failing without cython
    && pip3 install --no-cache-dir cython \
    && pip3 install --no-cache-dir -r requirements.txt \
    && rm requirements.txt

ENTRYPOINT ["/bin/sh"]