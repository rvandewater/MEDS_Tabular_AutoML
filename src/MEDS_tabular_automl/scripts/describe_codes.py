#!/usr/bin/env python
"""This Python script, stores the configuration parameters and feature columns used in the output."""
from collections import defaultdict
from pathlib import Path

import hydra
import numpy as np
import polars as pl
from loguru import logger
from omegaconf import DictConfig

from MEDS_tabular_automl.describe_codes import (
    compute_feature_frequencies,
    convert_to_df,
    convert_to_freq_dict,
)
from MEDS_tabular_automl.file_name import list_subdir_parquets
from MEDS_tabular_automl.mapper import wrap as rwlock_wrap
from MEDS_tabular_automl.utils import load_tqdm, store_config_yaml, write_df


@hydra.main(version_base=None, config_path="../configs", config_name="describe_codes")
def main(
    cfg: DictConfig,
):
    """Stores the configuration parameters and feature columns tabularized data we will be generated for.

    Args:
        cfg: The configuration object for the tabularization process.
    """
    iter_wrapper = load_tqdm(cfg.tqdm)

    # Store Config
    output_dir = Path(cfg.output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    store_config_yaml(output_dir / "config.yaml", cfg)

    # Create output dir
    input_dir = Path(cfg.input_dir)
    input_dir.mkdir(exist_ok=True, parents=True)

    # 0. Identify Output Columns and Frequencies
    logger.info("Iterating through shards and caching feature frequencies.")

    def compute_fn(shard_df):
        return compute_feature_frequencies(cfg, shard_df)

    def write_fn(df, out_fp):
        write_df(df, out_fp)

    def read_fn(in_fp):
        return pl.scan_parquet(in_fp)

    # Map: Iterates through shards and caches feature frequencies
    train_shards = list_subdir_parquets(cfg.input_dir)
    np.random.shuffle(train_shards)
    for shard_fp in iter_wrapper(train_shards):
        out_fp = Path(cfg.cache_dir) / shard_fp.name
        rwlock_wrap(
            shard_fp,
            out_fp,
            read_fn,
            write_fn,
            compute_fn,
            do_overwrite=cfg.do_overwrite,
            do_return=False,
        )

    logger.info("Summing frequency computations.")
    # Reduce: sum the frequency computations

    def compute_fn(freq_df_list):
        feature_freqs = defaultdict(int)
        for shard_freq_df in freq_df_list:
            shard_freq_dict = convert_to_freq_dict(shard_freq_df)
            for feature, freq in shard_freq_dict.items():
                feature_freqs[feature] += freq
        feature_df = convert_to_df(feature_freqs)
        return feature_df

    def write_fn(df, out_fp):
        write_df(df, out_fp)

    def read_fn(feature_dir):
        files = list_subdir_parquets(feature_dir)
        return [pl.scan_parquet(fp) for fp in files]

    rwlock_wrap(
        Path(cfg.cache_dir),
        Path(cfg.output_filepath),
        read_fn,
        write_fn,
        compute_fn,
        do_overwrite=cfg.do_overwrite,
        do_return=False,
    )
    logger.info("Stored feature columns and frequencies.")


if __name__ == "__main__":
    main()