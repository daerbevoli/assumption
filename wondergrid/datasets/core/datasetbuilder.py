from __future__ import annotations
from typing import Type

import os

from wondergrid.datasets.core.dataset import Dataset

DEFAULT_DATA_PATH = 'data'

class DatasetBuilder():

    def __init__(self):
        self.data_path = self.get_data_path()

    @classmethod
    def get_name(dataset_builder_cls: Type[DatasetBuilder]):
        return getattr(dataset_builder_cls, 'name')

    @classmethod
    def get_data_path(dataset_builder_cls: Type[DatasetBuilder]):
        dataset_name = dataset_builder_cls.get_name()
        data_path = os.getenv('DATA_PATH') or DEFAULT_DATA_PATH   
        data_path = os.path.join(data_path, dataset_name)
        if not os.path.exists(data_path):
            os.makedirs(data_path)
        return data_path
    
    @classmethod
    def load(dataset_builder_cls: Type[DatasetBuilder], **builder_kwargs) -> Dataset:
        builder = dataset_builder_cls(**builder_kwargs)
        builder.download_and_prepare()
        dataset = builder.load_as_dataset()
        return dataset
    
    def download_and_prepare(self) -> None:
        raise NotImplementedError()
    
    def load_as_dataset(self) -> Dataset:
        raise NotImplementedError()
