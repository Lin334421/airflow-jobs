from datetime import datetime

from airflow import DAG

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES
from oss_know.libs.github.sync_issues import sync_github_issues
from oss_know.libs.util.data_transfer import sync_clickhouse_repos_from_opensearch

with DAG(dag_id='daily_github_issues_sync',  # schedule_interval='*/5 * * * *',
         schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'daily sync']) as dag:
    raise DeprecationWarning("This DAG should not be used in production env, since the related comments and timeline"
                             " info should be updated when syncing issues, so issues sync should not run alone."
                             "\nRemove this deprecation code to run the DAG ONLY FOR DEBUG/TEST")
    opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
    clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
    table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
    github_issue_table_template = table_templates.get(OPENSEARCH_INDEX_GITHUB_ISSUES)

    github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
    proxy_accommodator = make_accommodator(github_tokens, proxy_confs, ProxyServiceProvider.Kuai,
                                           GithubTokenProxyAccommodator.POLICY_FIXED_MAP)


    def do_sync_github_issues_opensearch_group(owner_repo_group):
        for item in owner_repo_group:
            owner = item['owner']
            repo = item['repo']
            sync_github_issues(opensearch_conn_info, owner, repo, proxy_accommodator)
        return 'do_sync_github_issues:::end'


    def do_sync_github_issues_clickhouse_group(owner_repo_group):
        sync_clickhouse_repos_from_opensearch(owner_repo_group,
                                              OPENSEARCH_INDEX_GITHUB_ISSUES, opensearch_conn_info,
                                              OPENSEARCH_INDEX_GITHUB_ISSUES, clickhouse_conn_info,
                                              github_issue_table_template)


    uniq_owner_repos = Variable.get(DAILY_SYNC_GITHUB_ISSUES_INCLUDES, deserialize_json=True, default_var=None)
    if not uniq_owner_repos:
        uniq_owner_repos = get_uniq_owner_repos(clickhouse_conn_info, OPENSEARCH_INDEX_GITHUB_ISSUES)

    task_groups_by_capital_letter = arrange_owner_repo_into_letter_groups(uniq_owner_repos)
    # prev_op = None
    for letter, owner_repos in task_groups_by_capital_letter.items():
        op_sync_github_issues_opensearch = PythonOperator(
            task_id=f'op_sync_github_issues_opensearch_group_{letter}',
            python_callable=do_sync_github_issues_opensearch_group,
            trigger_rule='all_done',
            op_kwargs={
                "owner_repo_group": owner_repos
            }
        )
        op_sync_github_issues_clickhouse = PythonOperator(
            task_id=f'op_sync_github_issues_clickhouse_group_{letter}',
            python_callable=do_sync_github_issues_clickhouse_group,
            trigger_rule='all_done',
            op_kwargs={
                "owner_repo_group": owner_repos
            }
        )
        op_sync_github_issues_opensearch >> op_sync_github_issues_clickhouse
        # prev_op = op_sync_github_issues_clickhouse_group
