#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights & Biases
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script using this
    # particular version of the artifact
 
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    df = pd.read_csv(artifact_local_path)
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    logger.info("Removed any price outside of the {} to {} range".format(
                 args.min_price, args.max_price))
    df['last_review'] = pd.to_datetime(df['last_review'])
    logger.info("Put date in from last_review in pandas format")

    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    df.to_csv("clean_sample.csv", index=False)
    logger.info("Saved cleaned dataset locally")

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description
    )

    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

    artifact.wait()
    logger.info("Uploaded cleaned dataset to wandb")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This steps cleans the data")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help= "Insert the artifact input name",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Insert the artifact output name",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Insert the artifact output type",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Insert the artifact output description",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Insert the minimum price threshold",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Insert the maximum price threshold",
        required=True
    )

    args = parser.parse_args()

    go(args)
