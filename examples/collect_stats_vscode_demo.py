import uuid
import random
import tqdm
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).absolute().parent))

from df_engine.core.keywords import RESPONSE, TRANSITIONS
from df_engine.core import Context, Actor
import df_engine.conditions as cnd

from df_node_stats import Saver, Stats
from example_utils import parse_args


transitions = {
    "root": {
        "start": [
            "hi",
            "i like animals",
            "let's talk about animals",
        ]
    },
    "animals": {
        "have_pets": ["yes"],
        "like_animals": ["yes"],
        "what_animal": ["bird", "dog"],
        "ask_about_breed": ["pereat", "bulldog", "i do not known"],
    },
}
# a dialog script
script = {
    "root": {
        "start": {
            RESPONSE: "Hi",
            TRANSITIONS: {
                ("animals", "ask_some_questions"): cnd.exact_match("hi"),
                ("animals", "have_pets"): cnd.exact_match("i like animals"),
                ("animals", "like_animals"): cnd.exact_match("let's talk about animals"),
            },
        },
        "fallback": {RESPONSE: "Oops"},
    },
    "animals": {
        "ask_some_questions": {RESPONSE: "how are you"},
        "have_pets": {RESPONSE: "do you have pets?", TRANSITIONS: {"what_animal": cnd.exact_match("yes")}},
        "like_animals": {RESPONSE: "do you like it?", TRANSITIONS: {"what_animal": cnd.exact_match("yes")}},
        "what_animal": {
            RESPONSE: "what animals do you have?",
            TRANSITIONS: {
                "ask_about_color": cnd.exact_match("bird"),
                "ask_about_breed": cnd.exact_match("dog"),
            },
        },
        "ask_about_color": {RESPONSE: "what color is it"},
        "ask_about_breed": {
            RESPONSE: "what is this breed?",
            TRANSITIONS: {
                "ask_about_breed": cnd.exact_match("pereat"),
                "tell_fact_about_breed": cnd.exact_match("bulldog"),
                "ask_about_training": cnd.exact_match("i do not known"),
            },
        },
        "tell_fact_about_breed": {
            RESPONSE: "Bulldogs appeared in England as specialized bull-baiting dogs. ",
        },
        "ask_about_training": {RESPONSE: "Do you train your dog? "},
    },
}


def main(stats_object: Stats, n_iterations: int = 300):
    actor = Actor(script, start_label=("root", "start"), fallback_label=("root", "fallback"))

    stats_object.update_actor_handlers(actor, auto_save=False)
    ctxs = {}
    for i in tqdm.tqdm(range(n_iterations)):
        for j in range(4):
            ctx = ctxs.get(j, Context(id=uuid.uuid4()))
            label = ctx.last_label if ctx.last_label else actor.fallback_label
            flow, node = label[:2]
            if [flow, node] == ["root", "fallback"]:
                ctx = Context()
                flow, node = ["root", "start"]
            answers = list(transitions.get(flow, {}).get(node, []))
            in_text = random.choice(answers) if answers else "go to fallback"
            ctx.add_request(in_text)
            ctx = actor(ctx)
            ctx.clear(hold_last_n_indices=3)
            ctxs[j] = ctx
    return stats_object


if __name__ == "__main__":
    args = parse_args()
    saver = Saver("{type}://{user}:{password}@{host}:{port}/{name}".format(**args["db"]), table=args["db"]["table"])
    stats = Stats(saver, mock_dates=True)
    stats_object = main(stats)
    stats_object.save()
