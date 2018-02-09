FROM openjdk:8-jre

LABEL maintainer "Aloysius Lim"
LABEL maintainer "Fabio B. Silva <fabio.bat.silva@gmail.com>"
LABEL maintainer "Clive Zagno <clivez@gmail.com>"
LABEL maintainer "Joshua Lambert"
LABEL maintainer "Brian Flood"

# Build Args
ARG PDI_RELEASE="7.1"
ARG PDI_VERSION="7.1.0.0-12"
ARG PENTAHO_DIR="/opt/pentaho"
ARG PDI_HOME="$PENTAHO_DIR/data-integration"
ARG PDI_DOWNLOAD_URL="http://downloads.sourceforge.net/project/pentaho/Data%20Integration/$PDI_RELEASE/pdi-ce-$PDI_VERSION.zip"
ARG PG_CONNECTOR_VERSION="42.1.4"
ARG PG_CONNECTOR_DOWNLOAD_URL="https://jdbc.postgresql.org/download/postgresql-$PG_CONNECTOR_VERSION.jar"

# Read only variables
ENV PDI_PATH         "/etc/pdi"
ENV KETTLE_HOME      "$PDI_PATH"
ENV PDI_VERSION      "$PDI_VERSION"
ENV PATH             "$PATH:$PDI_HOME"
ENV CART_TEMPLATES   "$PENTAHO_DIR/templates/carte"
ENV KETTLE_TEMPLATES "$PENTAHO_DIR/templates/kettle"

# Set carte variables
ENV CARTE_NAME              "carte-server"
ENV CARTE_NETWORK_INTERFACE "eth0"
ENV CARTE_PORT              "8080"
ENV CARTE_USER              "cluster"
ENV CARTE_PASSWORD          "cluster"
ENV CARTE_IS_MASTER         "Y"
ENV CARTE_INCLUDE_MASTERS   "N"
# If CARTE_INCLUDE_MASTERS is 'Y', then these additional environment variables apply
ENV CARTE_REPORT_TO_MASTERS "Y"
ENV CARTE_MASTER_NAME       "carte-master"
ENV CARTE_MASTER_HOSTNAME   "localhost"
ENV CARTE_MASTER_PORT       "8080"
ENV CARTE_MASTER_USER       "cluster"
ENV CARTE_MASTER_PASSWORD   "cluster"
ENV CARTE_MASTER_IS_MASTER  "Y"

# Install deps
RUN apt-get update \
    && apt-get install -y git python python-pip gettext-base libwebkitgtk-1.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
RUN pip install requests psycopg2 logging ConfigParser dbt toolz

# Install PDI
RUN curl -L "$PDI_DOWNLOAD_URL" -o "/tmp/pdi-ce-$PDI_VERSION.zip" && \
    unzip -q "/tmp/pdi-ce-$PDI_VERSION.zip" -d "$PENTAHO_DIR" && \
    rm "/tmp/pdi-ce-$PDI_VERSION.zip" && \
    mkdir -p "/etc/entrypoint/conf.d" && \
    mkdir -p "$KETTLE_HOME/.kettle" && \
    mkdir -p "$PDI_PATH/carte" && \
    mkdir -p "$KETTLE_TEMPLATES" && \
    mkdir -p "$CART_TEMPLATES"

#Update the PostgreSQL Connector
RUN rm $PDI_HOME/lib/postgresql-* && \
    curl -L "$PG_CONNECTOR_DOWNLOAD_URL" -o "$PDI_HOME/lib/postgresql-$PG_CONNECTOR_VERSION.jar"

#Make project directory
RUN mkdir /bizops

# Workdir
WORKDIR "/bizops"
