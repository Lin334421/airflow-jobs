from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO, DAILY_SYNC_INTERVAL, \
    ROUTINELY_UPDATE_XXX_METRICS_INTERVAL, MYSQL_CONN_INFO, ROUTINELY_UPDATE_XXX_METRICS_INCLUDES
from oss_know.libs.metrics.influence_metrics import calculate_xxx_metrics, save_xxx_metrics
from oss_know.libs.util.base import arrange_owner_repo_into_letter_groups
from oss_know.libs.util.clickhouse import get_uniq_owner_repos

sync_interval = Variable.get(ROUTINELY_UPDATE_XXX_METRICS_INTERVAL, default_var=None)
if not sync_interval:
    sync_interval = Variable.get(DAILY_SYNC_INTERVAL, default_var=None)

mysql_conn_info = Variable.get(MYSQL_CONN_INFO, deserialize_json=True)

with DAG(dag_id='routinely_calculate_xxx_metrics',  # schedule_interval='*/5 * * * *',
         schedule_interval=sync_interval, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['metrics'], ) as dag:
    uniq_owner_repos = Variable.get(ROUTINELY_UPDATE_XXX_METRICS_INCLUDES, deserialize_json=True, default_var=None)
    if not uniq_owner_repos:
        clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        uniq_owner_repos = get_uniq_owner_repos(clickhouse_conn_info, OPENSEARCH_GIT_RAW)

    task_groups_by_capital_letter = arrange_owner_repo_into_letter_groups(uniq_owner_repos)


    def do_calculate_xxx_metrics(owner_repo_group):
        batch = []
        batch_size = 5000
        for (owner, repo) in owner_repo_group:
            metrics = calculate_xxx_metrics(owner, repo)
            batch += metrics

            if len(batch) >= batch_size:
                save_xxx_metrics(mysql_conn_info, metrics)
                batch = []

        if batch:
            save_xxx_metrics(mysql_conn_info, metrics)


    # TODO Currently the DAG makes 27 parallel task groups(serial execution inside each group)
    #  Check in production env if it works as expected(won't make too much pressure on opensearch and
    #  clickhouse. Another approach is to make all groups serial, one after another, which assign
    #  init_op to prev_op at the beginning, then assign op_sync_gits_clickhouse_group to prev_op
    #  in each loop iteration(as commented below)
    #  Another tip: though the groups dict is by default sorted by alphabet, the generated DAG won't
    #  respect the order
    # prev_op = op_init_daily_gits_sync
    for letter, owner_repos in task_groups_by_capital_letter.items():
        op_calculate_xxx_metrics = PythonOperator(
            task_id=f'op_calculate_xxx_metrics_{letter}',
            python_callable=do_calculate_xxx_metrics,
            trigger_rule='all_done',
            op_kwargs={
                "owner_repo_group": owner_repos
            }
        )
        # op_init_daily_gits_sync >> op_sync_gits_opensearch_group >> op_sync_gits_clickhouse_group
        # prev_op = op_sync_gits_clickhouse_group
