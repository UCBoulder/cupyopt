""" Factory for creating tasks from nuggets """
from collections import Callable

import prefect
import src.cupyopt.nuggets as nuggets


# prepare a placeholder class for prefect tasks created from nuggets
class NuggetTask:
    pass


# gather list of nuggets
nuggets_list = [
    f
    for f in dir(nuggets)
    if not f.startswith("__") and isinstance(getattr(nuggets, f), Callable)
]

# for each nugget, add it (and a parent) attr to NuggetTask
for nugget in nuggets_list:

    # parent attr for task
    parent = getattr(nuggets, nugget).__module__[20:]

    # if no parent attr attached to NuggetTask, set it
    if not hasattr(NuggetTask, parent,):
        setattr(
            NuggetTask, parent, lambda: None,
        )

    # set attr per parent attr within NuggetTask
    setattr(
        getattr(NuggetTask, parent,),
        getattr(nuggets, nugget).__name__,
        prefect.task(getattr(nuggets, nugget)),
    )
