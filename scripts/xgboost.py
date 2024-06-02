import json
import os
from collections.abc import Callable, Mapping
from pathlib import Path

import hydra
import numpy as np
import polars as pl
import scipy.sparse as sp
import xgboost as xgb
from mixins import TimeableMixin
from omegaconf import DictConfig, OmegaConf
from sklearn.metrics import roc_auc_score

from MEDS_tabular_automl.file_name import FileNameResolver
from MEDS_tabular_automl.utils import get_feature_indices, load_matrix

from loguru import logger

from datetime import datetime



class Iterator(xgb.DataIter, TimeableMixin):
    def __init__(self, cfg: DictConfig, split: str = "train"):
        """Initialize the Iterator with the provided configuration and split.

        Args:
        - cfg (DictConfig): Configuration dictionary.
        - split (str): The data split to use ("train", "tuning", or "held_out").
        """
        self.cfg = cfg
        self.file_name_resolver = FileNameResolver(cfg)
        self.split = split
        # self.data_path = Path(cfg.tabularized_data_dir)
        # self.dynamic_data_path = self.data_path / "sparse" / split
        # self.task_data_path = self.data_path / "task" / split
        self._data_shards = sorted(
            [shard.stem for shard in self.file_name_resolver.list_label_files(split)]
        )  # [2, 4, 5] #
        self.valid_event_ids, self.labels = self.load_labels()
        self.codes_set, self.num_features = self._get_code_set()
        feature_columns = json.load(open(self.file_name_resolver.get_feature_columns_fp()))
        self.agg_to_feature_ids = {agg: get_feature_indices(agg, feature_columns) for agg in cfg.aggs}

        self._it = 0

        # XGBoost will generate some cache files under current directory with the prefix
        # "cache"
        super().__init__(
            cache_prefix=os.path.join(".", "cache")
        )  # TODO: Change where this is!! it should be in the same directory it comes from!!
        #  this is security issue!

    @TimeableMixin.TimeAs
    def load_labels(self) -> tuple[Mapping[int, list], Mapping[int, list]]:
        """Loads valid event ids and labels for each shard.

        Returns:
        - Tuple[Mapping[int, list], Mapping[int, list]]: Tuple containing:
            dictionary from shard number to list of valid event ids -- used for indexing rows
                in the sparse matrix
            dictionary from shard number to list of labels for these valid event ids
        """
        label_fps = {
            shard: self.file_name_resolver.get_label(self.split, shard) for shard in self._data_shards
        }
        cached_labels, cached_event_ids = dict(), dict()
        for shard, label_fp in label_fps.items():
            label_df = pl.scan_parquet(label_fp)
            cached_event_ids[shard] = label_df.select(pl.col("event_id")).collect().to_series()

            # TODO: check this for Nan or any other case we need to worry about 
            cached_labels[shard] = label_df.select(pl.col("label")).collect().to_series()
            # if self.cfg.iterator.binarize_task:
            #     cached_labels[shard] = cached_labels[shard].map_elements(lambda x: 1 if x > 0 else 0, return_dtype=pl.Int8)
        
        return cached_event_ids, cached_labels

    @TimeableMixin.TimeAs
    def _get_code_set(self) -> set:
        """Get the set of codes to include in the data based on the configuration."""
        with open(self.file_name_resolver.get_feature_columns_fp()) as f:
            feature_columns = json.load(f)
        feature_dict = {col: i for i, col in enumerate(feature_columns)}
        if self.cfg.codes is not None:
            codes_set = {feature_dict[code] for code in set(self.cfg.codes) if code in feature_dict}

        if self.cfg.min_code_inclusion_frequency is not None:
            with open(self.file_name_resolver.get_feature_freqs_fp()) as f:
                feature_freqs = json.load(f)
            min_frequency_set = {
                key for key, value in feature_freqs.items() if value >= self.cfg.min_code_inclusion_frequency
            }
            frequency_set = {feature_dict[code] for code in min_frequency_set if code in feature_dict}

        if self.cfg.codes is not None and self.cfg.min_code_inclusion_frequency is not None:
            codes_set = codes_set.intersection(frequency_set)
        elif self.cfg.codes is not None:
            codes_set = codes_set
        elif self.cfg.min_code_inclusion_frequency is not None:
            codes_set = frequency_set
        else:
            codes_set = None  # set(feature_columns)
        # TODO: make sure we aren't filtering out static columns!!!
        return list(codes_set), len(feature_columns)

    @TimeableMixin.TimeAs
    def _load_dynamic_shard_from_file(self, path: Path, idx: int) -> sp.csc_matrix:
        """Load a sparse shard into memory.

        Args:
            - path (Path): Path to the sparse shard.

        Returns:
            - sp.coo_matrix: Data frame with the sparse shard.
        """
        # column_shard is of form event_idx, feature_idx, value
        matrix = load_matrix(path)
        if path.stem in ["first", "present"]:
            agg = f"static/{path.stem}"
        else:
            agg = f"{path.parent.stem}/{path.stem}"

        return self._filter_shard_on_codes_and_freqs(agg, sp.csc_matrix(matrix))

    @TimeableMixin.TimeAs
    def _get_dynamic_shard_by_index(self, idx: int) -> sp.csr_matrix:
        """Load a specific shard of dynamic data from disk and return it as a sparse matrix after filtering
        column inclusion.

        Args:
        - idx (int): Index of the shard to load.

        Returns:
        - sp.csr_matrix: Filtered sparse matrix.
        """
        # TODO Nassim Fix this guy
        # get all window_size x aggreagation files using the file resolver
        files = self.file_name_resolver.get_model_files(
            self.cfg.window_sizes, self.cfg.aggs, self.split, self._data_shards[idx]
        )
        if not all(file.exists() for file in files):
            raise ValueError("Not all files exist")
        
        shard_name = self._data_shards[idx]
        dynamic_csrs = [self._load_dynamic_shard_from_file(file, idx) for file in files]

        fn_name = "_get_dynamic_shard_by_index"
        hstack_key = f"{fn_name}/hstack"
        self._register_start(key=hstack_key)
        combined_csr = sp.hstack(dynamic_csrs, format="csr")  # TODO: check this
        self._register_end(key=hstack_key)
        # Filter Rows
        valid_indices = self.valid_event_ids[shard_name]
        filter_key = f"{fn_name}/filter"
        self._register_start(key=filter_key)
        out = combined_csr[valid_indices, :]
        self._register_end(key=filter_key)
        return out

    @TimeableMixin.TimeAs
    def _get_shard_by_index(self, idx: int) -> tuple[sp.csr_matrix, np.ndarray]:
        """Load a specific shard of data from disk and concatenate with static data.

        Args:
        - idx (int): Index of the shard to load.

        Returns:
        - X (scipy.sparse.csr_matrix): Feature data frame.ß
        - y (numpy.ndarray): Labels.
        """
        dynamic_df = self._get_dynamic_shard_by_index(idx)
        label_df = self.labels[self._data_shards[idx]]
        return dynamic_df, label_df

    @TimeableMixin.TimeAs
    def _filter_shard_on_codes_and_freqs(self, agg: str, df: sp.csc_matrix) -> sp.csc_matrix:
        """Filter the dynamic data frame based on the inclusion sets. Given the codes_mask, filter the data
        frame to only include columns that are True in the mask.

        Args:
        - df (scipy.sparse.coo_matrix): Data frame to filter.

        Returns:
        - df (scipy.sparse.sp.csr_matrix): Filtered data frame.
        """
        if self.codes_set is None:
            return df
        key=f"_filter_shard_on_codes_and_freqs/{agg}"
        self._register_start(key=key)
        feature_ids = self.agg_to_feature_ids[agg]
        code_mask = [True if idx in self.codes_set else False for idx in feature_ids]
        df = df[:, code_mask]  # [:, list({index for index in self.codes_set if index < df.shape[1]})]
        self._register_end(key=key)
        return df

    @TimeableMixin.TimeAs
    def next(self, input_data: Callable):
        """Advance the iterator by 1 step and pass the data to XGBoost.  This function is called by XGBoost
        during the construction of ``DMatrix``

        Args:
        - input_data (Callable): A function passed by XGBoost with the same signature as `DMatrix`.

        Returns:
        - int: 0 if end of iteration, 1 otherwise.
        """
        if self._it == len(self._data_shards):
            # return 0 to let XGBoost know this is the end of iteration
            return 0

        # input_data is a function passed in by XGBoost who has the exact same signature of
        # ``DMatrix``
        X, y = self._get_shard_by_index(self._it)  # self._data_shards[self._it])
        input_data(data=X, label=y)
        self._it += 1
        # Return 1 to let XGBoost know we haven't seen all the files yet.
        return 1

    @TimeableMixin.TimeAs
    def reset(self):
        """Reset the iterator to its beginning."""
        self._it = 0

    @TimeableMixin.TimeAs
    def collect_in_memory(self) -> tuple[sp.coo_matrix, np.ndarray]:
        """Collect the data in memory.

        Returns:
        - tuple[np.ndarray, np.ndarray]: Tuple of feature data and labels.
        """
        X = []
        y = []
        for i in range(len(self._data_shards)):
            X_, y_ = self._get_shard_by_index(i)
            X.append(X_)
            y.append(y_)

        X = sp.vstack(X)
        y = np.concatenate(y, axis=0)
        return X, y


class XGBoostModel(TimeableMixin):
    def __init__(self, cfg: DictConfig):
        """Initialize the XGBoostClassifier with the provided configuration.

        Args:
        - cfg (DictConfig): Configuration dictionary.
        """

        self.cfg = cfg
        self.keep_data_in_memory = getattr(getattr(cfg, "iterator", {}), "keep_data_in_memory", True)

        self.itrain = None
        self.ituning = None
        self.iheld_out = None

        self.dtrain = None
        self.dtuning = None
        self.dheld_out = None

        self.model = None
    @TimeableMixin.TimeAs
    def _train(self):
        """Train the model."""
        # TODO: add in eval, early stopping, etc.
        # TODO: check for Nan and inf in labels!
        self.model = xgb.train(
            OmegaConf.to_container(self.cfg.model), self.dtrain
        )   # TODO: fix eval etc. 
    @TimeableMixin.TimeAs
    def train(self):
        """Train the model."""
        self._build()
        self._train()

    @TimeableMixin.TimeAs
    def _build(self):
        """Build necessary data structures for training."""
        if self.keep_data_in_memory:
            self._build_iterators()
            self._build_dmatrix_in_memory()
        else:
            self._build_iterators()
            self._build_dmatrix_from_iterators()

    @TimeableMixin.TimeAs
    def _build_dmatrix_in_memory(self):
        """Build the DMatrix from the data in memory."""
        X_train, y_train = self.ituning.collect_in_memory()
        X_tuning, y_tuning = self.ituning.collect_in_memory()
        X_held_out, y_held_out = self.iheld_out.collect_in_memory()
        self.dtrain = xgb.DMatrix(X_train, label=y_train)
        self.dtuning = xgb.DMatrix(X_tuning, label=y_tuning)
        self.dheld_out = xgb.DMatrix(X_held_out, label=y_held_out)

    @TimeableMixin.TimeAs
    def _build_dmatrix_from_iterators(self):
        """Build the DMatrix from the iterators."""
        self.dtrain = xgb.DMatrix(self.itrain)
        self.dtuning = xgb.DMatrix(self.ituning)
        self.dheld_out = xgb.DMatrix(self.iheld_out)

    @TimeableMixin.TimeAs
    def _build_iterators(self):
        """Build the iterators for training, validation, and testing."""
        self.itrain = Iterator(self.cfg, split="train")
        self.ituning = Iterator(self.cfg, split="tuning")
        self.iheld_out = Iterator(self.cfg, split="held_out")

    @TimeableMixin.TimeAs
    def evaluate(self) -> float:
        """Evaluate the model on the test set.

        Returns:
        - float: Evaluation metric (mae).
        """
        # TODO: Figure out exactly what we want to do here

        y_pred = self.model.predict(self.dheld_out)
        y_true = self.dheld_out.get_label()
        return roc_auc_score(y_true, y_pred)


@hydra.main(version_base=None, config_path="../configs", config_name="xgboost")
def xgboost(cfg: DictConfig) -> float:
    """Optimize the model based on the provided configuration.

    Args:
    - cfg (DictConfig): Configuration dictionary.

    Returns:
    - float: Evaluation result.
    """
    model = XGBoostModel(cfg)
    model.train()
    logger.info("Time Profiling:")
    logger.info("Train Time:\n{}".format("\n".join(f"{key}: {value}" for key, value in model._profile_durations().items())))
    logger.info("Train Iterator Time:\n{}".format("\n".join(f"{key}: {value}" for key, value in model.itrain._profile_durations().items())))
    logger.info("Tuning Iterator Time:\n{}".format("\n".join(f"{key}: {value}" for key, value in model.ituning._profile_durations().items())))
    logger.info("Held Out Iterator Time:\n{}".format("\n".join(f"{key}: {value}" for key, value in model.iheld_out._profile_durations().items())))

    # print("Time Profiling:")
    # print("Train Time: \n", model._profile_durations())
    # print("Train Iterator Time: \n", model.itrain._profile_durations())
    # print("Tuning Iterator Time: \n", model.ituning._profile_durations())
    # print("Held Out Iterator Time: \n", model.iheld_out._profile_durations())

    # save model
    save_dir = Path(cfg.model_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    model.model.save_model(save_dir / "model.json")
    auc = model.evaluate()
    logger.info(f"ROC AUC: {auc}")
    return auc


if __name__ == "__main__":
    xgboost()
