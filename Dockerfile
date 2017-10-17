# Use ubuntu as a parent image
FROM ubuntu:16.04

SHELL ["/bin/sh", "-c"],

# add PostgreSQL package repository and GnuPG public key
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main" > /etc/apt/sources.list.d/pgdg.list

# update packages and install PostgreSQL and python
RUN apt-get update -q \
	&& DEBIAN_FRONTEND=noninteractive apt-get install -yq \
	build-essential \
	libffi-dev \
	libldap2-dev  \
	libsasl2-dev \
	libssl-dev \
	postgresql-10 \
	postgresql-client-10 \
	postgresql-contrib-10 \
	python \
	python-dev \
	python-pip \
	python-software-properties \
	software-properties-common \
	sudo

# Intall superset and postgres supprt 
RUN pip install \
	psycopg2 \
	superset

# create pguser -- we'll want to change this
USER postgres
RUN /etc/init.d/postgresql start && psql --command "CREATE USER pguser WITH SUPERUSER PASSWORD 'pguser';" && \
	createdb -O pguser pgdb && createdb superset

# Create data volumes and config folder
RUN mkdir -p /var/run/postgresql && chown -R postgres /var/run/postgresql 

USER root

# Expose postgres & web
EXPOSE 5432 8088 80

#Add Volumes
VOLUME  ["/etc/postgresql", "/var/log/postgresql", "/var/lib/postgresql"]

# Allow connections from all (we probbaly don't actually want to do this - but for now)
RUN echo "host all  all    0.0.0.0/0  md5" >> /etc/postgresql/10/main/pg_hba.conf && \
	echo "listen_addresses='*'" >> /etc/postgresql/10/main/postgresql.conf

# ADD Superset config to python path
ENV PYTHONPATH $PYTHONPATH:/opt/bizops/config/
COPY config/ /opt/bizops/config/
RUN chown -R postgres /opt/bizops/config && chmod 700 /opt/bizops/config/*
ENTRYPOINT "/opt/bizops/config/setup.sh"