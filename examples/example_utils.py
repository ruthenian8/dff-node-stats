import argparse
import os
import sys

from omegaconf import OmegaConf

common_opts = argparse.ArgumentParser(add_help=False)
common_opts.add_argument("-o", "--outfile", help="Name of the csv file to use instead of a database (if needed).")
common_opts.add_argument("-dP", "--db.password", required=True)

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest="cmd", description="Configuration source", required=True)
opts_parser = subparsers.add_parser("cfg_from_opts", parents=[common_opts])
opts_parser.add_argument("-dT", "--db.type", choices=["postgresql", "mysql+pymysql", "clickhouse"], required=True)
opts_parser.add_argument("-dU", "--db.user", required=True)
opts_parser.add_argument("-dh", "--db.host", required=True)
opts_parser.add_argument("-dp", "--db.port", required=True)
opts_parser.add_argument("-dn", "--db.name", required=True)
opts_parser.add_argument("-dt", "--db.table", required=True)
file_parser = subparsers.add_parser("cfg_from_file", parents=[common_opts])
file_parser.add_argument("file", type=str)


def parse_args():
    parsed_args = parser.parse_args(sys.argv[1:])
    sys.argv = [__file__] + [f"{key}={value}" for key, value in parsed_args.__dict__.items()]
    if parsed_args.cmd == "cfg_from_file":  # parse yaml input
        conf = OmegaConf.load(parsed_args.file)
        conf.merge_with_cli()
    elif parsed_args.cmd == "cfg_from_opts":
        conf = OmegaConf.from_cli()
    else:
        raise argparse.ArgumentError
    return conf._content
