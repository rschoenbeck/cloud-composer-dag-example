FROM ubuntu:18.04

WORKDIR /snow

# SnowSQL install destination, using root profile
ENV SNOWSQL_DEST=/snow
ENV SNOWSQL_LOGIN_SHELL=/root/.profile
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# Download SnowSQL via curl
RUN apt-get update && apt-get install -y \
   curl
RUN curl -o /snow/snowsql-1.1.76-linux_x86_64.bash http://s3-us-west-2.amazonaws.com/sfc-snowsql-updates/bootstrap/1.1/linux_x86_64/snowsql-1.1.76-linux_x86_64.bash
RUN chmod +x /snow/snowsql-1.1.76-linux_x86_64.bash
RUN /snow/snowsql-1.1.76-linux_x86_64.bash

# Set SnowSQL to be executable, create "files" dir
RUN chmod +x /snow/snowsql
# /files is the directory we will write output to, and is the volume
# mount directory used by the DAG to transfer data between DAG nodes
RUN mkdir /files

# Entrypoint not be needed running in Cloud Composer due to the DAG specifying the commands
