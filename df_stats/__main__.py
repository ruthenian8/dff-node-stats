"""
Main
*********
This module is a script designed to adapt the standard Superset dashboard to 
user-specific settings. Settings can be passed to the script with a config file
or as command line arguments.

Examples
**********

.. code:: bash

    df_stats cfg_from_file file.yaml --outfile=/tmp/superset_dashboard.zip

.. code:: bash

    df_stats cfg_from_opts \\
        --db.type=postgresql \\
        --db.user=root \\
        --db.host=localhost \\
        --db.port=5432 \\
        --db.name=${db.table} \\
        --db.table=dff_stats \\
        --outfile=/tmp/superset_dashboard.zip

.. code:: bash

    df_stats import_dashboard \\
        -U admin \\
        -P admin \\
        -i /tmp/superset_dashboard.zip \\
        -dP password

"""
import sys
import os
import argparse
import tempfile
import shutil
import logging
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
from typing import Optional

import requests
from omegaconf import OmegaConf

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest="cmd", description="update or import config", required=True)
opts_parser = subparsers.add_parser("cfg_from_opts")
opts_parser.add_argument(
    "-dT", "--db.type", choices=["postgresql", "mysql+mysqldb", "clickhousedb+connect"], required=True
)
opts_parser.add_argument("-dU", "--db.user", required=True)
opts_parser.add_argument("-dh", "--db.host", required=True)
opts_parser.add_argument("-dp", "--db.port", required=True)
opts_parser.add_argument("-dn", "--db.name", required=True)
opts_parser.add_argument("-dt", "--db.table", required=True)
opts_parser.add_argument("-o", "--outfile", required=True)
file_parser = subparsers.add_parser("cfg_from_file")
file_parser.add_argument("file", type=str)
file_parser.add_argument("-o", "--outfile", required=True)
import_parser = subparsers.add_parser("import_dashboard")
import_parser.add_argument("-U", "--username", required=True)
import_parser.add_argument("-P", "--password", required=True)
import_parser.add_argument("-dP", "--db.password", required=True)
import_parser.add_argument("-i", "--infile", required=True)

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
DASHBOARD_DIR = os.path.join(MODULE_DIR, "superset_dashboard")

TYPE_MAPPING_CH = {
    "FLOAT": "Nullable(Float64)",
    "STRING": "Nullable(String)",
    "LONGINTEGER": "Nullable(Int64)",
    "INTEGER": "Nullable(Int64)",
    "DATETIME": "Nullable(DateTime)",
}

SQL_STMT_MAPPING = {
    "dff_acyclic_nodes.yaml": "WITH main AS (\n  SELECT DISTINCT {table}.context_id, request_id, time, CAST({lblfield} AS {texttype}) AS label\n  \
    FROM {table} INNER JOIN \n  (\n    WITH helper AS \
    (\n         SELECT DISTINCT context_id, request_id, CAST({lblfield} AS {texttype}) \
    AS label from {table}\n         ) \n    SELECT context_id FROM helper GROUP BY context_id\n\
        HAVING count(context_id) = COUNT(DISTINCT label)\n  ) AS plain_ctx ON {table}.context_id \
    = plain_ctx.context_id\n     ORDER BY context_id, request_id\n     ) SELECT context_id, \
    request_id, time as start_time, label,\n    {lag} \
    AS prev_label\nFROM main;",
    "dff_node_stats.yaml": "WITH main AS (\n  SELECT context_id, request_id, time AS start_time, data_key, \
    data,\n   CAST({flowfield} AS {texttype}) AS flow_label, \n   CAST({nodefield} \
    AS {texttype}) AS node_label, \n   CAST({lblfield} AS {texttype}) AS label \n   FROM \
    {table} ORDER BY context_id, request_id)\nSELECT context_id, request_id, start_time, \
    data_key, CAST(data AS {texttype}) AS data, \nflow_label, node_label, label, {lag} AS prev_label\nFROM main;",
    "dff_final_nodes.yaml": "WITH main AS (SELECT context_id, max(request_id) AS max_hist FROM {table} GROUP \
    BY context_id)     \nSELECT {table}.*, CAST({flowfield} AS {texttype}) AS flow_label, \
    CAST({nodefield} AS {texttype}) AS node_label FROM {table} INNER JOIN main \nON {table}.context_id \
    = main.context_id AND {table}.request_id = main.max_hist;"
}


def addToZip(zf, path, zippath):
    """
    Recursively add files from a folder to a zip-archive.
    """
    if os.path.isfile(path):
        zf.write(path, zippath, ZIP_DEFLATED)
    elif os.path.isdir(path):
        if zippath:
            zf.write(path, zippath)
        for nm in sorted(os.listdir(path)):
            addToZip(zf, os.path.join(path, nm), os.path.join(zippath, nm))


def import_dashboard(parsed_args: argparse.Namespace = None):
    """
    Import an Apache Superset dashboard to a local instance with specified arguments.
    """
    zip_file = parsed_args.infile
    zip_filename = os.path.basename(zip_file)
    username = parsed_args.username
    password = parsed_args.password

    base_url = "http://localhost:8088"

    while True:
        try:
            response = requests.get(base_url, timeout=10)
            response.raise_for_status()
            break
        except Exception:
            pass

    login_url = f"{base_url}/login/"
    session = requests.Session()

    payload = {}
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
    }

    response = session.request("GET", login_url, headers=headers, data=payload)
    csrf_token = response.text.split('<input id="csrf_token" name="csrf_token" type="hidden" value="')[1].split('">')[0]

    payload = f"csrf_token={csrf_token}&username={username}&password={password}"
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    session.request("POST", login_url, headers=headers, data=payload, allow_redirects=False)
    session_cookie = session.cookies.get_dict().get("session")

    logger.info("Login sequence successful.")

    import_dashboard_url = f"{base_url}/api/v1/dashboard/import/"

    with open(zip_file, "rb") as f:
        payload = {
            "passwords": '{"databases/dff_database.yaml":"' + getattr(parsed_args, "db.password") + '"}',
            "overwrite": "true",
        }
        files = [("formData", (zip_filename, f, "application/zip"))]
        headers = {"Accept": "application/json", "Cookie": f"session={session_cookie}", "X-CSRFToken": csrf_token}

        response = requests.request("POST", import_dashboard_url, headers=headers, data=payload, files=files)
        response.raise_for_status()
        logger.info(f"Upload finished with status {response.status_code}.")


def make_zip_config(parsed_args: argparse.Namespace):
    """
    Make a zip-archived Apache Superset dashboard config, using specified arguments.
    """
    outfile_name = parsed_args.outfile

    if hasattr(parsed_args, "file") and parsed_args.file is not None:  # parse yaml input
        cli_conf = OmegaConf.load(parsed_args.file)
    else:
        sys.argv = [__file__] + [f"{key}={value}" for key, value in parsed_args.__dict__.items()]
        cli_conf = OmegaConf.from_cli()

    if OmegaConf.select(cli_conf, "db.type") == "clickhousedb+connect":
        params = dict(
            table = "${db.table}",
            lag = "neighbor(label, -1)",
            texttype = "String",
            lblfield = "JSON_VALUE(data, '$.label')",
            flowfield = "JSON_VALUE(data, '$.flow')",
            nodefield = "JSON_VALUE(data, '$.node')",
        )
    else:
        params = dict(
            table = "${db.table}",
            lag = "LAG(label,1) OVER (ORDER BY context_id, request_id)",
            texttype = "TEXT",
            lblfield = "data -> 'label'",
            flowfield = "data -> 'flow'",
            nodefield = "data -> 'node'"
        )

    conf = SQL_STMT_MAPPING.copy()
    for key in conf.keys():
        conf[key] = {}
        conf[key]["sql"] = SQL_STMT_MAPPING[key].format(**params)

    resolve_conf = OmegaConf.create(
        {
            "database": {
                "sqlalchemy_uri": "${db.type}://${db.user}:XXXXXXXXXX@${db.host}:${db.port}/${db.name}",
            },
            **conf
        }
    )

    user_config = OmegaConf.merge(cli_conf, resolve_conf)
    OmegaConf.resolve(user_config)

    with tempfile.TemporaryDirectory() as temp_config_dir:
        nested_temp_dir = os.path.join(temp_config_dir, "superset_dashboard")
        logger.info(f"Copying config files to temporary directory: {nested_temp_dir}.")

        shutil.copytree(DASHBOARD_DIR, nested_temp_dir)
        database_dir = Path(os.path.join(nested_temp_dir, "databases"))
        dataset_dir = Path(os.path.join(nested_temp_dir, "datasets/dff_database"))

        logger.info(f"Overriding the initial configuration.")
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
                    col.type = TYPE_MAPPING_CH.get(col.type, col.type)
            OmegaConf.save(new_file_config, filepath)

        logger.info(f"Saving the archive to {outfile_name}.")

        zip_args = {}
        if sys.version >= "3.8":
            zip_args["strict_timestamps"] = False

        with ZipFile(outfile_name, "w", **zip_args) as zf:
            zippath = os.path.basename(nested_temp_dir)
            if not zippath:
                zippath = os.path.basename(os.path.dirname(nested_temp_dir))
            if zippath in ("", os.curdir, os.pardir):
                zippath = ""
            addToZip(zf, nested_temp_dir, zippath)


def main(parsed_args: Optional[argparse.Namespace] = None):
    if parsed_args is None:
        parsed_args = parser.parse_args(sys.argv[1:])

    if not hasattr(parsed_args, "outfile"):  # get outfile
        import_dashboard(parsed_args)
        return

    make_zip_config(parsed_args)


if __name__ == "__main__":
    main()
