from typing import Type

from wondergrid.datasets.core.datasetbuilder import DatasetBuilder

DATASET_REGISTRY: dict[str, Type[DatasetBuilder]] = {}


def register_dataset_builder_cls(dataset_builder_cls: Type[DatasetBuilder]):
    name = dataset_builder_cls.get_name()
    DATASET_REGISTRY[name] = dataset_builder_cls

def get_dataset_builder_cls(name: str) -> Type[DatasetBuilder]:
    if not name in DATASET_REGISTRY:
        raise DatasetBuilderNotFoundError(f"No dataset builder with name '{name}' found in registry.")
    return DATASET_REGISTRY.get(name)


class DatasetBuilderNotFoundError(ValueError):
    pass