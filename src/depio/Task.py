from __future__ import annotations

from os.path import getmtime
from typing import List, Callable, get_origin, Annotated, get_args
import os

class Product():
    pass

class Dependency():
    pass

class ProductNotProducedException(Exception):
    pass

class ProductNotUpdatedException(Exception):
    pass

class DependencyNotMetException(Exception):
    pass

# from https://stackoverflow.com/questions/218616/how-to-get-method-parameter-names
def _get_args_dict(fn, args, kwargs):
    args_names = fn.__code__.co_varnames[:fn.__code__.co_argcount]
    return {**dict(zip(args_names, args)), **kwargs}

def _parse_annotation_for_metaclass(func, metaclass):
    annotations = getattr(func, "__annotations__", {})
    results = []

    for name, annotation in annotations.items():
        if get_origin(annotation) is Annotated:
            args = get_args(annotation)
            if len(args) <= 1:
                continue

            metadata = args[1:]
            if any(meta is metaclass for meta in metadata):
                results.append(name)

    return results

class Task:
    def __init__(self, name: str, func: Callable, dependencies_hard: List[Task] = None, func_args: List = None, func_kwargs: List = None, ):
        self.name = name
        self.func = func
        self.func_args = func_args or []
        self.func_kwargs = func_kwargs or {}
        self.dependencies_hard = dependencies_hard or []

        self.products_args = _parse_annotation_for_metaclass(func, Product)
        self.dependencies_args = _parse_annotation_for_metaclass(func, Dependency)

        args_dict = _get_args_dict(func, self.func_args, self.func_kwargs)

        self.products = [args_dict[argname] for argname in self.products_args]
        self.dependencies = [args_dict[argname] for argname in self.dependencies_args]


    def run(self):
        d = [str(dependency) for dependency in self.dependencies if dependency.exists()]
        if any(not b for b in d):
            raise ProductNotProducedException(f"Task {self.name}: Dependency/ies {d} not met.")

        # Store the last-modification timestamp of the already existing products.
        pt_before = [(str(product),getmtime(product)) for product in self.products if product.exists()]

        # Call the actual function
        self.func(*self.func_args, **self.func_kwargs)

        # Check if any product does not exist.
        p = [str(product) for product in self.products if not product.exists()]
        if len(p) > 0:
            raise ProductNotProducedException(f"Task {self.name}: Product/s {p} not produced.")

        # Check if any product has not been updated.
        pt_after = [(str(product),getmtime(product)) for product in self.products]
        not_updated_products = []
        for before, after in zip(pt_before, pt_after):
            if before[0] == after[0] and before[1] == after[1]:
                not_updated_products.append(before[0])

        if len(not_updated_products) > 0:
            raise ProductNotUpdatedException(f"Task {self.name}: Product/s {not_updated_products} not updated.")


__all__ = [Task, Product, Dependency]
