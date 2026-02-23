import click
import pandas as pd

from constants.network_constants import Chains
from jobs.transactions_retriever import TransactionsExporter
from utils.logger_utils import get_logger

logger = get_logger("Export Transactions")


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-c",
    "--chain",
    default="bsc",
    show_default=True,
    type=str,
    help="Network name example bsc or polygon",
)
@click.option("-e", "--end-block", type=int, help="End block")
@click.option("-s", "--start-block", type=int, help="Start block")
@click.option(
    "-w",
    "--max-workers",
    default=8,
    show_default=True,
    type=int,
    help="The number of workers",
)
@click.option(
    "-b", "--batch-size", default=100, show_default=True, type=int, help="Batch size"
)
def export_transactions(chain, end_block, start_block, max_workers, batch_size):
    """Export a selected set of transactions, for easy & quick query compared to querying from Mongo Main"""
    chain = str(chain).lower()
    if chain not in Chains.mapping:
        raise click.BadOptionUsage("--chain", f"Chain {chain} is not support")
    chain_id = Chains.mapping[chain]
    df = pd.read_csv(f"./data/{chain_id}_wallets_pairs.csv")

    logger.info("Export x wallets")
    x_wallets = list(df["x"])
    transactions_retriever = TransactionsExporter(
        wallets_list=x_wallets,
        end_block=end_block,
        start_block=start_block,
        batch_size=batch_size,
        max_workers=max_workers,
        chain_id=chain_id,
    )
    transactions_retriever.run()

    logger.info("Export y wallets")
    y_wallets = list(df["y"])
    transactions_retriever = TransactionsExporter(
        wallets_list=y_wallets,
        end_block=end_block,
        start_block=start_block,
        batch_size=batch_size,
        max_workers=max_workers,
        chain_id=chain_id,
    )
    transactions_retriever.run()
