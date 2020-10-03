# Building pbspro-test
# docker build -f Dockerfile -t pbspro-test:latest .

# Run with
# docker run -it --rm --name pbs -h pbs -e PBS_START_MOM=1 pbspro-test:latest "qsub -- /bin/sleep && qstat"

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

# Install mpi
RUN yum install gcc gcc-c++ gcc-gfortran make wget -y \
    && wget http://www.mpich.org/static/downloads/3.3.2/mpich-3.3.2.tar.gz \
    && tar -xvzf mpich-3.3.2.tar.gz \
    && cd mpich-3.3.2 \
    && ./configure --prefix=/usr \
    && make \
    && make install \
    && rm -rf mpich*

# Change the start-script in order to enter a started cluster
# First we remove the executions at the end of the script
RUN sed -i "s/exec \"\$@\"//" entrypoint.sh
# NOTE: The "-" in "su -" is equivalent to a normal login, so that the paths etc. are updated
RUN sed -i "s/$SU_PART/\n$CREATE_PBS_NODE\n$SET_QUEUE\n$RUN_CMD_AS_PBSUSER/" entrypoint.sh
RUN cat entrypoint.sh

ENTRYPOINT ["bash", "/entrypoint.sh"]
