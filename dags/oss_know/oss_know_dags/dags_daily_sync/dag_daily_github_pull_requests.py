from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS
from oss_know.libs.base_dict.variable_key import GITHUB_TOKENS, OPENSEARCH_CONN_DATA, PROXY_CONFS, \
    DAILY_SYNC_GITHUB_PRS_EXCLUDES, CLICKHOUSE_DRIVER_INFO, CK_TABLE_DEFAULT_VAL_TPLT, \
    DAILY_SYNC_GITHUB_PRS_INCLUDES, DAILY_SYNC_INTERVAL, DAILY_GITHUB_PRS_SYNC_INTERVAL
from oss_know.libs.github.sync_pull_requests import sync_github_pull_requests
from oss_know.libs.util.base import get_opensearch_client, arrange_owner_repo_into_letter_groups
from oss_know.libs.util.clickhouse import get_uniq_owner_repos
from oss_know.libs.util.data_transfer import sync_clickhouse_repos_from_opensearch
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.util.proxy import GithubTokenProxyAccommodator, ProxyServiceProvider, make_accommodator

clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
github_pr_table_template = table_templates.get(OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS)

sync_interval = Variable.get(DAILY_GITHUB_PRS_SYNC_INTERVAL, default_var=None)
if not sync_interval:
    sync_interval = Variable.get(DAILY_SYNC_INTERVAL, default_var=None)

with DAG(dag_id='daily_github_pull_requests_sync',  # schedule_interval='*/5 * * * *',
         schedule_interval=sync_interval, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'daily sync']) as dag:
    opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

    github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
    proxy_accommodator = make_accommodator(github_tokens, proxy_confs, ProxyServiceProvider.Kuai,
                                           GithubTokenProxyAccommodator.POLICY_FIXED_MAP)


    def do_sync_github_pull_requests_opensearch_group(owner_repo_group):
        for item in owner_repo_group:
            owner = item['owner']
            repo = item['repo']
            sync_github_pull_requests(opensearch_conn_info, owner, repo, proxy_accommodator)


    def do_sync_github_pull_requests_clickhouse_group(owner_repo_group):
        sync_clickhouse_repos_from_opensearch(owner_repo_group,
                                              OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS, opensearch_conn_info,
                                              OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS, clickhouse_conn_info,
                                              github_pr_table_template)


    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    uniq_owner_repos = Variable.get(DAILY_SYNC_GITHUB_PRS_INCLUDES, deserialize_json=True, default_var=None)
    if not uniq_owner_repos:
        excludes = Variable.get(DAILY_SYNC_GITHUB_PRS_EXCLUDES, deserialize_json=True, default_var=None)
        uniq_owner_repos = get_uniq_owner_repos(clickhouse_conn_info, OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS)

    task_groups_by_capital_letter = arrange_owner_repo_into_letter_groups(uniq_owner_repos)
    # prev_op = None
    for letter, owner_repos in task_groups_by_capital_letter.items():
        op_sync_github_pr_opensearch = PythonOperator(
            task_id=f'op_sync_github_pull_requests_opensearch_group_{letter}',
            python_callable=do_sync_github_pull_requests_opensearch_group,
            trigger_rule='all_done',
            op_kwargs={
                "owner_repo_group": owner_repos
            }
        )
        op_sync_github_pr_clickhouse = PythonOperator(
            task_id=f'op_sync_github_pull_requests_clickhouse_group_{letter}',
            python_callable=do_sync_github_pull_requests_clickhouse_group,
            trigger_rule='all_done',
            op_kwargs={
                "owner_repo_group": owner_repos
            }
        )
        op_sync_github_pr_opensearch >> op_sync_github_pr_clickhouse
        # prev_op = op_sync_github_pr_clickhouse
