#!/usr/bin/env -S uv run
# -*- coding: utf-8 -*-

import argparse
import json
import logging
from pathlib import Path
import polars as pl
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import f1_score
from xgboost import XGBClassifier
import xgboost as xgb
import shap
import optuna

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="XGBoost Feature Importance Analysis"
    )
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        required=True,
        help="Path to input Parquet file",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        required=True,
        help="Path to results JSON file",
    )
    parser.add_argument(
        "-y", "--year", type=int, required=True, help="Year to be analyzed"
    )
    parser.add_argument(
        "-t",
        "--threshold",
        type=float,
        default=0.05,
        help="Threshold for underrepresented classes",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=-1,
        help="Number of parallel jobs (default: -1 for all cores)",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--optuna-trials",
        type=int,
        default=200,
        help="Maximum number of Optuna trials (default: 200)",
    )
    parser.add_argument(
        "--optuna-timeout",
        type=int,
        default=36000,
        help="Timeout for Optuna trials in seconds (default: 10h)",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug logging"
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    if not args.input.exists() or not args.input.is_file():
        msg = f"Input file {args.input} does not exist."
        logger.error(msg)
        raise FileNotFoundError(msg)

    features = [
        "Flow Duration",
        "Total Fwd Packet",
        "Total Bwd packets",
        "Total Length of Fwd Packet",
        "Total Length of Bwd Packet",
        "Fwd Packet Length Max",
        "Fwd Packet Length Min",
        "Fwd Packet Length Mean",
        "Fwd Packet Length Std",
        "Bwd Packet Length Max",
        "Bwd Packet Length Min",
        "Bwd Packet Length Mean",
        "Bwd Packet Length Std",
        "Flow Bytes/s",
        "Flow Packets/s",
        "Flow IAT Mean",
        "Flow IAT Std",
        "Flow IAT Max",
        "Flow IAT Min",
        "Fwd IAT Total",
        "Fwd IAT Mean",
        "Fwd IAT Std",
        "Fwd IAT Max",
        "Fwd IAT Min",
        "Bwd IAT Total",
        "Bwd IAT Mean",
        "Bwd IAT Std",
        "Bwd IAT Max",
        "Bwd IAT Min",
        "Fwd PSH Flags",
        "Bwd PSH Flags",
        "Fwd URG Flags",
        "Bwd URG Flags",
        "Fwd RST Flags",
        "Bwd RST Flags",
        "Fwd Header Length",
        "Bwd Header Length",
        "Fwd Packets/s",
        "Bwd Packets/s",
        "Packet Length Min",
        "Packet Length Max",
        "Packet Length Mean",
        "Packet Length Std",
        "Packet Length Variance",
        "FIN Flag Count",
        "SYN Flag Count",
        "RST Flag Count",
        "PSH Flag Count",
        "ACK Flag Count",
        "URG Flag Count",
        "CWR Flag Count",
        "ECE Flag Count",
        "Down/Up Ratio",
        "Average Packet Size",
        "Fwd Segment Size Avg",
        "Bwd Segment Size Avg",
        "Fwd Bytes/Bulk Avg",
        "Fwd Packet/Bulk Avg",
        "Fwd Bulk Rate Avg",
        "Bwd Bytes/Bulk Avg",
        "Bwd Packet/Bulk Avg",
        "Bwd Bulk Rate Avg",
        "Subflow Fwd Packets",
        "Subflow Fwd Bytes",
        "Subflow Bwd Packets",
        "Subflow Bwd Bytes",
        "FWD Init Win Bytes",
        "Bwd Init Win Bytes",
        "Fwd Act Data Pkts",
        "Bwd Act Data Pkts",
        "Fwd Seg Size Min",
        "Bwd Seg Size Min",
        "Active Mean",
        "Active Std",
        "Active Max",
        "Active Min",
        "Idle Mean",
        "Idle Std",
        "Idle Max",
        "Idle Min",
        "ICMP Code",
        "ICMP Type",
        "Fwd TCP Retrans. Count",
        "Bwd TCP Retrans. Count",
        "Total TCP Retrans. Count",
        "Total Connection Flow Time",
    ]
    target = "taxonomy_norm"

    logger.info(f"Reading data from {args.input}")
    # Read parquet file as polars lazy frame and filter for year
    lf_data = pl.scan_parquet(args.input)

    # Filter for the specified year
    lf_year = lf_data.filter(pl.col("year") == args.year)
    lf_features = lf_year.select(features + [target, "split"])

    df_train = lf_features.filter(pl.col("split") == "train").collect()
    if df_train.is_empty():
        msg = f"No training data found for year {args.year}."
        logger.error(msg)
        raise ValueError(msg)
    logger.info(f"Loaded train dataframe with shape: {df_train.shape}")

    target_counts = (
        df_train[target]
        .value_counts()
        .sort(by="count", descending=True, nulls_last=True)
    )
    logger.debug(f"{target_counts=}")
    keep_taxonomies = target_counts.filter(
        pl.col("count") >= target_counts["count"].max() * args.threshold
    )[target].to_list()
    logger.warning(
        f"The following taxonomies are removed due to underrepresentation:\n{set(target_counts[target].to_list()) - set(keep_taxonomies)}"
    )
    # remove target that are less than 10% of max target count
    df_train = df_train.filter(pl.col(target).is_in(keep_taxonomies))

    logger.info(
        f"Filtered dataframe to shape {df_train.shape} after removing rare classes"
    )
    target_counts = (
        df_train[target]
        .value_counts()
        .sort(by="count", descending=True, nulls_last=True)
    )
    logger.debug(f"{target_counts=}")

    df_val = lf_features.filter(
        (pl.col("split") == "valid") & (pl.col(target).is_in(keep_taxonomies))
    ).collect()
    if df_val.is_empty():
        msg = f"No validation data found for year {args.year}."
        logger.error(msg)
        raise ValueError(msg)
    logger.info(f"Validation dataframe shape: {df_val.shape}")
    df_test = lf_features.filter(
        (pl.col("split") == "test") & (pl.col(target).is_in(keep_taxonomies))
    ).collect()
    if df_test.is_empty():
        msg = f"No test data found for year {args.year}."
        logger.error(msg)
        raise ValueError(msg)
    logger.info(f"Test dataframe shape: {df_test.shape}")

    # Convert to numpy arrays for sklearn
    X_train = df_train.select(features).to_numpy()
    y_train = df_train.select(target).to_numpy().ravel()
    X_val = df_val.select(features).to_numpy()
    y_val = df_val.select(target).to_numpy().ravel()
    X_test = df_test.select(features).to_numpy()
    y_test = df_test.select(target).to_numpy().ravel()

    le = LabelEncoder()
    y_train = le.fit_transform(y_train)
    y_val = le.transform(y_val)
    y_test = le.transform(y_test)
    class_names = le.classes_
    logger.debug(f"Classes: {class_names}")

    # Optuna hyperparameter tuning
    def objective(trial):
        params = {
            "objective": "multi:softprob",
            "num_class": len(class_names),
            "eval_metric": "mlogloss",
            "tree_method": "hist",
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.15),
            "max_depth": trial.suggest_int("max_depth", 3, 10),
            "n_estimators": trial.suggest_int("n_estimators", 500, 2000),
            "min_child_weight": trial.suggest_int("min_child_weight", 1, 20),
            "subsample": trial.suggest_float("subsample", 0.6, 0.95),
            "colsample_bytree": trial.suggest_float(
                "colsample_bytree", 0.6, 0.95
            ),
            "reg_lambda": trial.suggest_float(
                "reg_lambda", 1.0, 50.0, log=True
            ),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
            "random_state": args.seed,
            "n_jobs": args.jobs,
        }

        clf = XGBClassifier(
            **params,
            callbacks=[xgb.callback.EarlyStopping(rounds=100, save_best=True)],
        )
        clf.fit(
            X_train,
            y_train,
            eval_set=[(X_train, y_train), (X_val, y_val)],
            verbose=True if args.debug else False,
        )
        y_pred = clf.predict(X_val)

        labels_present = np.unique(y_val)
        macro_f1 = f1_score(
            y_val,
            y_pred,
            average="macro",
            labels=labels_present,
            zero_division=0,
        )
        return macro_f1

    logger.info("Starting Optuna hyperparameter tuning...")
    study = optuna.create_study(
        direction="maximize",
        pruner=optuna.pruners.MedianPruner(n_startup_trials=10),
    )
    study.optimize(
        objective, n_trials=args.optuna_trials, timeout=args.optuna_timeout
    )
    logger.info(f"Best trial: {study.best_trial.value}")
    logger.info(f"Best params: {study.best_trial.params}")

    # Use best params for final model
    static_params = {
        "objective": "multi:softprob",
        "num_class": len(class_names),
        "eval_metric": "mlogloss",
        "tree_method": "hist",
        "random_state": args.seed,
        "n_jobs": args.jobs,
    }
    best_params = study.best_trial.params
    clf = XGBClassifier(
        **{**static_params, **best_params},
        callbacks=[xgb.callback.EarlyStopping(rounds=100, save_best=True)],
    )
    clf.fit(
        X_train,
        y_train,
        eval_set=[(X_train, y_train), (X_val, y_val)],
        verbose=True if args.debug else False,
    )

    # Get training history
    train_loss = clf.evals_result()["validation_0"]["mlogloss"]
    val_loss = clf.evals_result()["validation_1"]["mlogloss"]

    # Calculate F1 score
    y_pred = clf.predict(X_test)
    f1 = f1_score(y_test, y_pred, average="macro")

    logger.info(f"Final training loss: {train_loss[-1]:.4f}")
    logger.info(f"Final validation loss: {val_loss[-1]:.4f}")
    logger.info(f"F1 score (macro): {f1:.4f}")

    explainer = shap.TreeExplainer(clf)
    shap_vals = explainer.shap_values(X_test)

    # Ensure shap_arr is (N, C, F)
    if isinstance(shap_vals, list):
        # list of C arrays, each (N, F) -> stack to (N, C, F)
        shap_arr = np.stack(shap_vals, axis=1)
    else:
        shap_arr = np.asarray(shap_vals)
        if shap_arr.ndim == 3:
            N, A, B = shap_arr.shape
            # If it's (N, F, C), swap to (N, C, F)
            if A == len(features) and B == len(class_names):
                shap_arr = np.transpose(shap_arr, (0, 2, 1))
            # If it's already (N, C, F), leave it
        else:
            # Binary edge case: (N, F) -> treat as 2 classes with neg/pos
            shap_arr = shap_arr[
                :, None, :
            ]  # (N, 1, F); handle separately if needed

    # Now this is always (N, C, F)
    per_class_imp = np.abs(shap_arr).mean(axis=0)  # (C, F)

    per_class_df = pd.DataFrame(
        per_class_imp, columns=features, index=class_names
    )

    macro_imp = per_class_imp.mean(axis=0)
    macro_df = pd.DataFrame(
        {"feature": features, "macro_importance": macro_imp}
    ).sort_values("macro_importance", ascending=False)

    class_weights = np.bincount(y_test) / len(y_test)
    weighted_imp = np.average(per_class_imp, axis=0, weights=class_weights)
    weighted_df = pd.DataFrame(
        {"feature": features, "weighted_importance": weighted_imp}
    ).sort_values("weighted_importance", ascending=False)

    results = {
        "input": str(args.input),
        "year": args.year,
        "threshold": args.threshold,
        "seed": args.seed,
        "xgb_params": {**static_params, **best_params},
        "n_train": len(X_train),
        "n_val": len(X_val),
        "n_test": len(X_test),
        "n_features": len(features),
        "n_classes": len(class_names),
        "class_names": class_names.tolist(),
        "features": features,
        "train_loss": train_loss,
        "val_loss": val_loss,
        "f1_score": f1,
        "per_class_importance": per_class_df.to_dict(orient="index"),
        "macro_importance": macro_df.to_dict(orient="records"),
        "weighted_importance": weighted_df.to_dict(orient="records"),
    }

    # top 10 macro features
    logger.info(f"Top 10 macro features:\n{macro_df.head(10)}")

    # Write results to JSON file
    args.output.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Writing results to {args.output}")
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)

    logger.info("Analysis completed successfully")


if __name__ == "__main__":
    main()
