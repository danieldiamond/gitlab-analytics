FROM postgres:alpine

COPY setup/init-user.sh /docker-entrypoint/initdb.d/init-user.sh
