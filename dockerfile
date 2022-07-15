FROM superset:latest
COPY . .
USER root
ENV SUPERSET_FEATURE_EMBEDDED_SUPERSET=true
RUN superset db upgrade
RUN pip install sqlalchemy-clickhouse
RUN superset import_datasources -p superset_dashboard -r
EXPOSE 8088
USER superset 