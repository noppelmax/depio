from typing import Annotated
import pathlib
import inspect

from icecream import ic

from depio.BuildMode import BuildMode
from depio.Executors import SequentialExecutor
from depio.Pipeline import Pipeline
from depio.Task import Product, Dependency, Task

BLD = pathlib.Path("build")
BLD.mkdir(exist_ok=True)

depioExecutor = SequentialExecutor()
defaultpipeline = Pipeline(depioExecutor=depioExecutor, clear_screen=False)


class Resolver:
    def __init__(self, BLD):
        self.BLD = BLD

    @property
    def test_input_path(self) -> pathlib.Path:
        return BLD / "input.txt"

    @property
    def test_output_path(self) -> pathlib.Path:
        return BLD / "output.txt"

    def __call__(self, fn, args, kwargs):

        # Get the function signature
        sig = inspect.signature(fn)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()  # Fill in default values

        # Lets to the positional arguments first
        for name, value in bound_args.arguments.items():
            if value is None and getattr(self, name):
                bound_args.arguments[name] = getattr(self, name)

        # Now we do the kwargs
        for name, value in bound_args.kwargs.items():
            if value is None and getattr(self, name):
                bound_args.arguments[name] = getattr(self, name)

        return bound_args.args, bound_args.kwargs




def gen_input_data( BLD: pathlib.Path,
                   test_input_path: Annotated[pathlib.Path, Product] = None):

    with open(test_input_path, 'w') as f:
        f.write("Hallo from depio")


def gen_output_data( BLD: pathlib.Path,
                   test_input_path: Annotated[pathlib.Path, Dependency] = None,
                   test_output_path: Annotated[pathlib.Path, Product] = None):

    ic(test_input_path)
    ic(test_output_path)

    with open(test_output_path,'w') as f:
        f.write("Hallo from depio")


resolver = Resolver(BLD)

print("Adding tasks")
defaultpipeline.add_task(Task("Generate Input Data", gen_input_data, [BLD],
                              buildmode=BuildMode.ALWAYS, arg_resolver=resolver))
defaultpipeline.add_task(Task("Generate Output Data", gen_output_data, [BLD],
                              buildmode=BuildMode.ALWAYS, arg_resolver=resolver))


exit(defaultpipeline.run())
