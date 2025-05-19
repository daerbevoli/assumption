from wondergrid.datasets.core.dataset import Dataset
from wondergrid.datasets.core.datasetbuilder import DatasetBuilder
from wondergrid.datasets.core.registry import get_dataset_builder_cls


def load_dataset(name: str, **builder_kwargs) -> Dataset:
    builder = create_dataset_builder(name, **builder_kwargs)
    builder.download_and_prepare()
    dataset = builder.load_as_dataset()
    return dataset

def create_dataset_builder(name: str, **builder_kwargs) -> DatasetBuilder:
    dataset_builder_cls = get_dataset_builder_cls(name)
    return dataset_builder_cls(**builder_kwargs)