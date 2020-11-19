# Building pbspro-test
# *NOTE* must be built from root of this repo due to setup.cfg
# docker build -f Dockerfile -t pbspro-test:latest -f docker/PBS/Dockerfile .

# Run with
# docker run -it --rm -h pbs -e PBS_START_MOM=1 pbspro-test:latest "qsub -- /bin/sleep && qstat"

# Use PBSPro built from
# https://hub.docker.com/r/pbspro/pbspro/
# And the following guide
# https://openpbs.atlassian.net/wiki/spaces/PBSPro/pages/79298561/Using+Docker+to+Instantiate+PBS
FROM pbspro/pbspro:latest

ENV PBS_START_MOM=1

# The change of user comes before calling the commands in the original script
ARG SU_PART="&& su - pbsuser"
ARG CREATE_PBS_NODE="\/opt\/pbs\/bin\/qmgr -c \"create node pbs\""
ARG SET_QUEUE="\/opt\/pbs\/bin\/qmgr -c \"set node pbs queue=workq\""
# How to run code as a different user
# https://stackoverflow.com/a/1988265/2786884
# NOTE: "$@" expands to "$1" "$2" ..., whereas "$*" expands to "$1 $2 ..."
# https://unix.stackexchange.com/questions/41571/what-is-the-difference-between-and#
ARG RUN_CMD_AS_PBSUSER="su -c \"\$*\" - \"pbsuser\""

# Install depenencies
RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum-config-manager --enable epel
RUN yum install centos-release-scl -y
RUN yum install devtoolset-7-gcc* -y
# For some reason mpich2 seems not to be installing properly
RUN yum install \
    make \
    which \
    git \
    fftw3-devel \
    netcdf-devel \
    netcdf-cxx-devel \
    openmpi \
    openmpi-devel \
    wget \
    -y

# Add python to the path
# This is needed for pip install under
# Unfortunately this will not add anaconda to the path, so we do it again in profile.d
ENV PATH="/opt/anaconda/bin:${PATH}"

# Install python3 and dependencies
COPY setup.cfg .
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && chmod +x Miniconda3-latest-Linux-x86_64.sh \
    && ./Miniconda3-latest-Linux-x86_64.sh -b -p /opt/anaconda \
    && python -c "import configparser; c = configparser.ConfigParser(); c.read('setup.cfg'); print(c['options']['install_requires'], c['options.extras_require']['test'])" | xargs pip install \
    && rm -rf setup.cfg Miniconda3-latest-Linux-x86_64.sh

# Set the python path for all users (appending it directly to /etc/profile hides other paths)
# https://forums.centos.org/viewtopic.php?t=60887
# As we are installing OpenMPI rather than MPICH2, we need to export the executable
# In addition we add the new gcc path as the standard does not contain c++-14
RUN echo "export PATH=/opt/anaconda/bin:$PATH" > /etc/profile.d/bout-specific.sh \
    && echo "export PATH=/usr/lib64/openmpi/bin:$PATH" >> /etc/profile.d/bout-specific.sh \
    && echo ". /opt/rh/devtoolset-7/enable" >> /etc/profile.d/bout-specific.sh \
    && chmod +x /etc/profile.d/bout-specific.sh

# Change the start-script in order to enter a started cluster
# First we remove the executions at the end of the script
RUN sed -i "s/exec \"\$@\"//" entrypoint.sh
# NOTE: The "-" in "su -" is equivalent to a normal login, so that the paths etc. are updated
RUN sed -i "s/$SU_PART/\n$CREATE_PBS_NODE\n$SET_QUEUE\n$RUN_CMD_AS_PBSUSER/" entrypoint.sh

ENTRYPOINT ["bash", "/entrypoint.sh"]
