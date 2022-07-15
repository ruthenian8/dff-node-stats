# Statistics collection extension for Dialog Flow Framework
dff_node_stats is package, that extends basic [dialog_flow_engine](https://github.com/deepmipt/dialog_flow_engine) by adding statistic collection **and** dashboard for visualization.

# Installation
Installation:
```bash
# install dialog flow framework
pip install dff_engine
# Install dff_node_stats
pip install dff_node_stats #basic
```
# Register a callback for stats collection

Insert stats in your dff code:
```python
# import dependencies
from df_engine.core.script import Script
from df_engine.core.actor import Actor
from dff_node_stats import Stats, Saver
# ....
# Define a plot and an actor
script = Script(foo)
actor = Actor(script=script, start_node=("a", "b"), fallback_node=("b", "c"))

# Define a destination for stats saving
stats = Stats(
    saver=Saver("csv://examples/stats.csv")
)
# As an alternative, you can use a database. Currently, Clickhouse and Postgres are supported
stats = Stats(
    saver=Saver("postgresql://user:password@localhost:5432/default")
)

# Add handlers to actor
stats.update_actor_handlers(actor, auto_save=False)

# ....
# Handle user requests
# ....
```

# Configure and launch an Apache Superset dashboard

```bash
# you can pass configuration settings via parameters 
python configure.py opts --db.type=postgresql --db.user=user --db.password=pass --db.host=localhost --db.port=8322 --db.name=default --db.table=some
# or write them to a file and pass it to parameters
python configure.py cfg_file config.yaml
```

# Run Examples:
```bash
# run dff dialog bot and collect stats
python examples/1.collect_stats.py
# or this one, they have differences only in a dialog scripts
python examples/1.collect_stats_vscode_demo.py

```