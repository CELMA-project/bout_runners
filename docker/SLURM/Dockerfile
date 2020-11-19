# Building slurm-test
# *NOTE* must be built from root of this repo due to setup.cfg
# docker build -f Dockerfile -t slurm-test:latest -f docker/SLURM/Dockerfile .

# Run with
# docker run -it -h ernie --rm slurm-test:latest "sbatch --wrap 'sleep 60' && squeue && sleep 10 && sacct"

# Use SLURM built from
# https://github.com/giovtorres/docker-centos7-slurm
FROM giovtorres/docker-centos7-slurm:latest

# Install depenencies
RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum-config-manager --enable epel
RUN yum install centos-release-scl -y
RUN yum install devtoolset-7-gcc* -y
# For some reason mpich2 seems not to be installing properly
RUN yum install \
    make \
    which \
    openmpi \
    openmpi-devel \
    fftw3-devel \
    netcdf-devel \
    netcdf-cxx-devel \
    -y

# Set correct locale in order to interpret utf-8 correctly
RUN localedef -c -f UTF-8 -i en_US en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# Install dependencies
COPY setup.cfg .
RUN pip3 install --upgrade pip \
    && python3 -c "import configparser; c = configparser.ConfigParser(); c.read('setup.cfg'); print(c['options']['install_requires'], c['options.extras_require']['test'])" | xargs pip3 install \
    && rm -rf setup.cfg

# Set the python path for all users (appending it directly to /etc/profile hides other paths)
# https://forums.centos.org/viewtopic.php?t=60887
# As we are installing OpenMPI rather than MPICH2, we need to export the executable
# In addition we add the new gcc path as the standard does not contain c++-14
RUN echo "export PATH=/opt/anaconda/bin:$PATH" > /etc/profile.d/bout-specific.sh \
    && echo "export PATH=/usr/lib64/openmpi/bin:$PATH" >> /etc/profile.d/bout-specific.sh \
    && echo ". /opt/rh/devtoolset-7/enable" >> /etc/profile.d/bout-specific.sh \
    && chmod +x /etc/profile.d/bout-specific.sh

# Extend the entrypoint due to https://github.com/giovtorres/docker-centos7-slurm/issues/3
RUN sed -i "s/exec \"\$@\"//" /usr/local/bin/docker-entrypoint.sh

# NOTE: Cannot change /etc/hosts during docker build
# Update /etc/hosts according to https://github.com/giovtorres/docker-centos7-slurm/issues/1
RUN echo "# Add processors to /etc/hosts" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "echo '127.0.0.1 c1' >> /etc/hosts" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "echo '127.0.0.1 c2' >> /etc/hosts" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "echo '127.0.0.1 c3' >> /etc/hosts" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "echo '127.0.0.1 c4' >> /etc/hosts" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "echo '127.0.0.1 c5' >> /etc/hosts" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "# Setup sacct" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "sleep 10" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "sacctmgr --immediate add cluster name=linux" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "supervisorctl restart slurmdbd" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "supervisorctl restart slurmctld" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "sleep 5" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "sacctmgr --immediate add account none,test Cluster=linux Description=\"none\" Organization=\"none\"" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "# create default non-root user" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "adduser slurmuser" >> /usr/local/bin/docker-entrypoint.sh \
    && echo "su -c \"\$*\" - \"slurmuser\"" >> /usr/local/bin/docker-entrypoint.sh
