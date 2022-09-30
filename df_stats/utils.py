from df_runner import WrapperRuntimeInfo

STATS_KEY = "STATS_KEY"


def get_wrapper_field(info: WrapperRuntimeInfo, postfix: str = "") -> str:
    """
    This function can be used to obtain a key, under which the wrapper data will be stored
    in the context.
    """
    return f"{info['component']['path']}" + (f"-{postfix}" if postfix else "")
