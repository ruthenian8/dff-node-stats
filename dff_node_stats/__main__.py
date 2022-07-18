from collections import namedtuple
import sys
import os
from pathlib import Path
import argparse
from zipfile import main as zip_main

from omegaconf import OmegaConf


module_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

database_dir = Path(os.path.join(module_dir, "superset_dashboard/databases"))
dataset_dir = Path(os.path.join(module_dir, "superset_dashboard/datasets/dff_database"))

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()
opts_parser = subparsers.add_parser("opts")
opts_parser.add_argument("--db.type", choices=["postgresql", "clickhousedb+connect"], required=True)
opts_parser.add_argument("--db.user", required=True)
opts_parser.add_argument("--db.host", required=True)
opts_parser.add_argument("--db.port", required=True)
opts_parser.add_argument("--db.name", required=True)
opts_parser.add_argument("--db.table", required=True)
file_parser = subparsers.add_parser("cfg_file")
file_parser.add_argument("file", type=str)

type_mapping_ch = {
    "FLOAT": "Nullable(Float64)",
    "STRING": "Nullable(String)",
    "LONGINTEGER": "Nullable(Int64)",
    "INTEGER": "Nullable(Int64)",
    "DATETIME": "Nullable(DateTime)",
}


sql_stmt_mapping = {
    "dff_node_stats.yaml": "WITH main AS (\n  SELECT context_id, history_id,\n  start_time, duration_time,\n \
    flow_label, node_label, attitude, full_label as label\n \
    FROM ${db.table} ORDER BY context_id, history_id)\n SELECT context_id, history_id,\n  start_time, \
    duration_time,\n  flow_label, node_label, label, CAST(attitude AS Integer) as attitude,\n  lag as prev_label\n \
    FROM main;",
    "dff_acyclic_nodes.yaml": "WITH main as (\n  SELECT DISTINCT ${db.table}.context_id, history_id, start_time, \
    full_label as label\n  FROM ${db.table} INNER JOIN \n  (\n    WITH helper as (\n \
        SELECT DISTINCT context_id, history_id, full_label as label from ${db.table}\n \
        ) \n    SELECT context_id FROM helper GROUP BY context_id\n    HAVING count(context_id) \
    = COUNT(DISTINCT label)\n  ) as plain_ctx ON ${db.table}.context_id = plain_ctx.context_id\n \
    ORDER BY context_id, history_id\n \
    ) SELECT context_id, history_id, start_time, label,\n\
    lag as prev_label\nFROM main;",
    "dff_final_nodes.yaml": "WITH main AS (SELECT context_id, max(history_id) as max_hist FROM ${db.table} GROUP BY context_id) \
    \nSELECT ${db.table}.* FROM ${db.table} INNER JOIN main \nON ${db.table}.context_id \
    = main.context_id AND ${db.table}.history_id = main.max_hist;",
}


def main(parsed_args):
    if hasattr(parsed_args, "file") and parsed_args.file is not None:
        cli_conf = OmegaConf.load(parsed_args.file)
    else:
        sys.argv = [item.strip("-") for item in sys.argv]
        cli_conf = OmegaConf.from_cli()

    if OmegaConf.select(cli_conf, "db.type") == "clickhousedb+connect":
        lag = "neighbor(label, -1)"
    else:
        lag = "LAG(label,1) OVER (ORDER BY context_id, history_id)"

    resolve_conf = OmegaConf.create(
        {
            "database": {
                "sqlalchemy_uri": "${db.type}://${db.user}:XXXXXXXXXX@${db.host}:${db.port}/${db.name}",
            },
            **{key: {"sql": value.replace("lag", lag)} for key, value in sql_stmt_mapping.items()},
        }
    )

    user_config = OmegaConf.merge(cli_conf, resolve_conf)
    OmegaConf.resolve(user_config)

    # overwrite sqlalchemy uri
    for filepath in database_dir.iterdir():
        file_config = OmegaConf.load(filepath)
        new_file_config = OmegaConf.merge(file_config, OmegaConf.select(user_config, "database"))
        OmegaConf.save(new_file_config, filepath)

    # overwrite sql expressions and column types
    for filepath in dataset_dir.iterdir():
        file_config = OmegaConf.load(filepath)
        new_file_config = OmegaConf.merge(file_config, getattr(user_config, filepath.name))
        if OmegaConf.select(cli_conf, "db.type") == "clickhousedb+connect":
            for col in OmegaConf.select(new_file_config, "columns"):
                col.type = type_mapping_ch.get(col.type, col.type)
        OmegaConf.save(new_file_config, filepath)

    zip_args = ["-c", "superset_dashboard.zip", os.path.join(module_dir, "superset_dashboard/")]
    zip_main(zip_args)


if __name__ == "__main__":
    parsed_args = parser.parse_args(sys.argv[1:])
    main(parsed_args)
