from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import GIT_SAVE_LOCAL_PATH, CLICKHOUSE_DRIVER_INFO, DAILY_SYNC_INTERVAL
from oss_know.libs.github.code_owner import LLVMCodeOwnerWatcher, PytorchCodeOwnerWatcher, KernelCodeOwnerWatcher, \
    K8SCodeOwnerWatcher

git_repo_path = Variable.get(GIT_SAVE_LOCAL_PATH, deserialize_json=True)
ck_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
sync_interval = Variable.get(DAILY_SYNC_INTERVAL, default_var=None)

with DAG(dag_id='dag_watch_code_owners', schedule_interval=sync_interval,
         start_date=datetime(2021, 1, 1), catchup=False, tags=['analysis'], concurrency=5) as dag:
    def do_watch(watcher_class, rev_list_params):
        watcher = watcher_class(git_repo_path['PATH'], ck_conn_info, rev_list_params=rev_list_params)
        watcher.init()
        watcher.watch()


    for cls, extra_params in [
        (LLVMCodeOwnerWatcher, []),
        (PytorchCodeOwnerWatcher, []),
        (KernelCodeOwnerWatcher, ['--since=01-01-2022']),
        (K8SCodeOwnerWatcher, []),
    ]:
        owner = cls.OWNER
        repo = cls.REPO
        operator = PythonOperator(
            task_id=f'watch_{owner}_{repo}',
            python_callable=do_watch,
            op_kwargs={
                'watcher_class': cls,
                'rev_list_params': extra_params,
            },
        )
