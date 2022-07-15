import pathlib
import sys
from pathlib import Path
import argparse

from omegaconf import OmegaConf

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()
opts_parser = subparsers.add_parser("opts")
opts_parser.add_argument("--db.type", required=True)
opts_parser.add_argument("--db.user", required=True)
opts_parser.add_argument("--db.password", required=True)
opts_parser.add_argument("--db.host", required=True)
opts_parser.add_argument("--db.port", required=True)
opts_parser.add_argument("--db.name", required=True)
opts_parser.add_argument("--db.table", required=True)
file_parser = subparsers.add_parser("cfg_file")
file_parser.add_argument("file", type=str)

sql = "WITH main AS (\n  SELECT context_id, history_id,\n  start_time, duration_time,\n\
\  flow_label, node_label, attitude, full_label as label\n\
\ FROM ${db.table} ORDER BY context_id, history_id)\n SELECT context_id, history_id,\n  start_time,\
\ duration_time,\n  flow_label, node_label, label, CAST(attitude AS INT),\n  lag as prev_label\n\
\ FROM main;"
no_cycles = "WITH main as (\n  SELECT DISTINCT ${db.table}.context_id, history_id, start_time,\
\ full_label as label\n  FROM ${db.table} INNER JOIN \n  (\n    WITH helper as (\n\
\      SELECT DISTINCT context_id, history_id, full_label as label from ${db.table}\n\
\    ) \n    SELECT context_id FROM helper GROUP BY context_id\n    HAVING count(context_id)\
\ = COUNT(DISTINCT label)\n  ) as plain_ctx ON ${db.table}.context_id = plain_ctx.context_id\n\
) SELECT context_id, history_id, start_time, label,\n\
\ lag as prev_label\nFROM main;"
terminal = "WITH main AS (SELECT context_id, max(history_id) FROM ${db.table} GROUP BY context_id)\
\ \nSELECT ${db.table}.* FROM ${db.table} INNER JOIN main \nON ${db.table}.context_id\
\ = main.context_id AND ${db.table}.history_id = main.max;"


def main(parsed_args):
    if hasattr(parsed_args, "file") and parsed_args.file is not None:
        cli_conf = OmegaConf.load(parsed_args.file)
    else:
        sys.argv = [item.strip("-") for item in sys.argv]
        cli_conf = OmegaConf.from_cli()

    if OmegaConf.select(cli_conf, "db.type") == "clickhouse":
        lag = "neighbor(full_label, -1)"
    else:
        lag = "LAG(full_label,1) OVER (ORDER BY context_id, history_id)"
    
    resolve_conf = OmegaConf.create(
        {
            "database": {
                "sqlalchemy_uri": "${db.type}://${db.user}:${db.password}@${db.host}:${db.port}/${db.name}",
            },
            "main": {
                "sql": sql.replace("lag", lag)
            },
            "acyclic": {
                "sql": no_cycles.replace("lag", lag)
            },
            "terminal": {
                "sql": terminal
            }
        }
    )
    user_config = OmegaConf.merge(cli_conf, resolve_conf)
    OmegaConf.resolve(user_config)
    database_dir = Path("superset_dashboard/databases")
    dataset_dir = Path("superset_dashboard/datasets/dff_database")
    # overwrite sqlalchemy uri
    for filepath in database_dir.iterdir():
        file_config = OmegaConf.load(filepath)
        new_file_config = OmegaConf.merge(file_config, OmegaConf.select(user_config, "database"))
        print(OmegaConf.to_yaml(new_file_config))
        # OmegaConf.save(new_file_config, filepath)
    for filepath in dataset_dir.iterdir():
        
        if filepath.name == "dff_acyclic_nodes.yaml":
            key = "acyclic"
        elif filepath.name == "dff_node_stats.yaml":
            key = "main"
        else:
            key = "terminal"
        
        file_config = OmegaConf.load(filepath)
        new_file_config = OmegaConf.merge(file_config, OmegaConf.select(user_config, key))
        print(OmegaConf.to_yaml(new_file_config))
        # OmegaConf.save(new_file_config, filepath)
        


if __name__ == "__main__":
    parsed_args = parser.parse_args(sys.argv[1:])
    main(parsed_args)
