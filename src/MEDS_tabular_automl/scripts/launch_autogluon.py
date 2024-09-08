from importlib.resources import files
from pathlib import Path

import hydra
import pandas as pd
from loguru import logger
from omegaconf import DictConfig

from MEDS_tabular_automl.tabular_dataset import TabularDataset as DenseIterator

from ..utils import hydra_loguru_init

config_yaml = files("MEDS_tabular_automl").joinpath("configs/launch_autogluon.yaml")
if not config_yaml.is_file():
    raise FileNotFoundError("Core configuration not successfully installed!")


@hydra.main(version_base=None, config_path=str(config_yaml.parent.resolve()), config_name=config_yaml.stem)
def main(cfg: DictConfig) -> float:
    """Launches AutoGluon after collecting data based on the provided configuration.

    Args:
        cfg: The configuration dictionary specifying model and training parameters.
    """

    # print(OmegaConf.to_yaml(cfg))
    if not cfg.loguru_init:
        hydra_loguru_init()

    # check that autogluon is installed
    try:
        import autogluon.tabular as ag
    except ImportError:
        logger.error("AutoGluon is not installed. Please install AutoGluon.")

    # collect data based on the configuration
    itrain = DenseIterator(cfg, "train")
    ituning = DenseIterator(cfg, "tuning")
    iheld_out = DenseIterator(cfg, "held_out")

    # collect data for AutoGluon
    train_data, train_labels = itrain.densify()
    tuning_data, tuning_labels = ituning.densify()
    held_out_data, held_out_labels = iheld_out.densify()

    # construct dfs for AutoGluon
    train_df = pd.DataFrame(train_data.todense())  # , columns=cols)
    train_df[cfg.task_name] = train_labels
    tuning_df = pd.DataFrame(
        tuning_data.todense(),
    )  # columns=cols)
    tuning_df[cfg.task_name] = tuning_labels
    held_out_df = pd.DataFrame(held_out_data.todense())  # , columns=cols)
    held_out_df[cfg.task_name] = held_out_labels

    train_dataset = ag.TabularDataset(train_df)
    tuning_dataset = ag.TabularDataset(tuning_df)
    held_out_dataset = ag.TabularDataset(held_out_df)

    # train model with AutoGluon
    predictor = ag.TabularPredictor(
        label=cfg.task_name, log_to_file=True, log_file_path=cfg.log_filepath, path=cfg.output_filepath
    ).fit(train_data=train_dataset, tuning_data=tuning_dataset)

    # predict
    predictions = predictor.predict(held_out_dataset.drop(columns=[cfg.task_name]))
    logger.info("Predictions:", predictions)
    # evaluate
    score = predictor.evaluate(held_out_dataset)
    logger.info("Test score:", score)

    log_fp = Path(cfg.model_log_dir)
    log_fp.mkdir(parents=True, exist_ok=True)
    # log hyperparameters
    out_fp = log_fp / "trial_performance_results.log"
    with open(out_fp, "w") as f:
        f.write(f"{cfg.output_filepath}\t{cfg.tabularization}\t{cfg.model_params}\t{None}\t{score}\n")


if __name__ == "__main__":
    main()
