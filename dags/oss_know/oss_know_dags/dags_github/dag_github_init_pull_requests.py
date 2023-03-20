from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import GITHUB_TOKENS, OPENSEARCH_CONN_DATA, \
    NEED_INIT_GITHUB_PULL_REQUESTS_REPOS, PROXY_CONFS
from oss_know.libs.util.proxy import GithubTokenProxyAccommodator, ProxyServiceProvider, \
    make_accommodator

# v0.0.1
with DAG(
        dag_id='github_init_pull_requests_v1',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_init_github_pull_requests(ds, **kwargs):
        return 'End:scheduler_init_github_pull_requests'


    op_scheduler_init_github_pull_requests = PythonOperator(
        task_id='op_scheduler_init_github_pull_requests',
        python_callable=scheduler_init_github_pull_requests
    )


    def do_init_github_pull_requests(params):
        from airflow.models import Variable
        from oss_know.libs.github import init_pull_requests

        opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

        github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
        proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
        proxy_accommodator = make_accommodator(github_tokens, proxy_confs, ProxyServiceProvider.Kuai,
                                               GithubTokenProxyAccommodator.POLICY_FIXED_MAP)

        owner = params["owner"]
        repo = params["repo"]
        since = None
        init_pull_requests.init_sync_github_pull_requests(opensearch_conn_info, owner, repo, proxy_accommodator, since)

        return params


    need_do_init_sync_ops = []

    from airflow.models import Variable

    need_init_github_pull_requests_repos = Variable.get(NEED_INIT_GITHUB_PULL_REQUESTS_REPOS, deserialize_json=True)

    for init_github_pull_requests_repo in need_init_github_pull_requests_repos:
        owner = init_github_pull_requests_repo["owner"]
        repo = init_github_pull_requests_repo["repo"]

        op_do_init_github_pull_requests = PythonOperator(
            task_id=f'op_do_init_github_pull_requests_{owner}_{repo}',
            python_callable=do_init_github_pull_requests,
            op_kwargs={'params': init_github_pull_requests_repo},
        )
        op_scheduler_init_github_pull_requests >> op_do_init_github_pull_requests
