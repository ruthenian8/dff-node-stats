FROM apache/superset
USER root
RUN pip install clickhouse-connect
RUN chown -R superset:superset /app
RUN chmod 755 /app
COPY . /app/dialog_flow_node_stats/
RUN pip install dialog_flow_node_stats/
USER superset