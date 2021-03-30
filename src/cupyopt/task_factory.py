""" Factory for creating tasks from nuggets """
from collections import Callable

import prefect
import src.cupyopt.nuggets as nuggets

# prepare a placeholder class for prefect tasks created from nuggets
ptask = lambda: None

# gather list of nuggets
nuggets_list = [
    f
    for f in dir(nuggets)
    # exclude built-ins and include only functions
    if not f.startswith("__") and isinstance(getattr(nuggets, f), Callable)
]

# for each nugget, add it (and a parent) attr to ptask
for nugget in nuggets_list:

    # parent attr for task
    parent = getattr(nuggets, nugget).__module__[20:]

    # if no parent attr attached to ptask, set it
    if not hasattr(ptask, parent):
        setattr(
            ptask,
            parent,
            lambda: None,
        )

    # set attr per parent attr within ptask
    setattr(
        getattr(ptask, parent),
        getattr(nuggets, nugget).__name__,
        prefect.task(getattr(nuggets, nugget)),
    )
