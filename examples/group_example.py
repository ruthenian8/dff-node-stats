import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).absolute().parent))

from df_runner import Pipeline, CLIMessengerInterface
from df_stats import Stats, Saver

from example_utils import parse_args, script

if __name__ == "__main__":
    args = parse_args()

    saver = Saver("{type}://{user}:{password}@{host}:{port}/{name}".format(**args["db"]), table=args["db"]["table"])

    stats = Stats(saver=saver, mock_dates=True)

    pipeline = Pipeline.from_script(
        script,
        ("root", "start"),
        ("root", "fallback"),
        {},
        CLIMessengerInterface(),
        [stats.get_start_time],
        [stats.collect_and_save_stats],
    )
    pipeline.run()
