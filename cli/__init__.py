# import click

# from cli.deposit_reuse.deposits_and_users_collect import deposits_and_users_collect
# from cli.deposit_reuse.exchange_deposit_wallets import exchange_deposit_wallets
# from cli.export_transactions import export_transactions
# from cli.name_service.graph_ens_enrich import graph_ens_enrich
# from cli.name_service.mongo_ens_export import mongo_ens_export
# from cli.subgraph_exporter import subgraph_exporter
# from cli.wallet_graph.graph_exporter import graph_exporter
# from cli.wallet_graph.graph_prune import graph_prune


# @click.group()
# @click.version_option(version="1.0.0")
# @click.pass_context
# def cli(ctx):
#     # Command line
#     pass


# cli.add_command(exchange_deposit_wallets, "exchange_deposit_wallets")
# cli.add_command(export_transactions, "export_transactions")
# cli.add_command(graph_exporter, "graph_exporter")
# cli.add_command(graph_prune, "graph_prune")
# cli.add_command(subgraph_exporter, "subgraph_exporter")
# cli.add_command(deposits_and_users_collect, "deposits_and_users_collect")
# cli.add_command(mongo_ens_export, "mongo_ens_export")
# cli.add_command(graph_ens_enrich, "graph_ens_enrich")
