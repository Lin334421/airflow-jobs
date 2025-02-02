import json
from datetime import datetime

import clickhouse_driver

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_PROFILE
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.clickhouse_driver import CKServer
from oss_know.libs.util.data_transfer import opensearch_to_clickhouse
from oss_know.libs.util.log import logger

# TODO: Make the fail over timeout configurable
FAIL_OVER_TIMEOUT_SETTING_SQL = "set connect_timeout_with_failover_ms = 100000"


def combine_remote_owner_repos(local_ck_conn_info, remote_ck_conn_info, table_name, combination_type='union'):
    remote_uniq_owner_repos_sql = f"""
    select distinct(search_key__owner, search_key__repo)
    from remote(
            '{remote_ck_conn_info["HOST"]}:{remote_ck_conn_info["PORT"]}',
            '{remote_ck_conn_info["DATABASE"]}.{table_name}',
            '{remote_ck_conn_info["USER"]}',
            '{remote_ck_conn_info["PASSWD"]}'
        )
    """

    uniq_owner_repos_sql = f"""
    select distinct(search_key__owner, search_key__repo)
    from {table_name}
    """
    ck_client = CKServer(host=local_ck_conn_info.get("HOST"),
                         port=local_ck_conn_info.get("PORT"),
                         user=local_ck_conn_info.get("USER"),
                         password=local_ck_conn_info.get("PASSWD"),
                         database=local_ck_conn_info.get("DATABASE"),
                         settings={
                             "max_execution_time": 1000000,
                         },
                         kwargs={
                             "connect_timeout": 200,
                             "send_receive_timeout": 6000,
                             "sync_request_timeout": 100,
                         })
    ck_client.execute_no_params(FAIL_OVER_TIMEOUT_SETTING_SQL)

    # TODO Extend the clickhouse client to provide __enter__ mechanism
    # So the block can be wrapped inside a with statement to close the client
    # Another consideration: return the same type

    logger.info(f'Get owner-repo pairs combination with type {combination_type}')

    # Types could be: union, intersection, only_local, only_remote
    if combination_type == 'only_local':
        local_owner_repos = [tup[0] for tup in ck_client.execute_no_params(uniq_owner_repos_sql)]
        ck_client.close()
        return local_owner_repos

    if combination_type == 'only_remote':
        remote_owner_repos = [tup[0] for tup in ck_client.execute_no_params(remote_uniq_owner_repos_sql)]
        ck_client.close()
        return remote_owner_repos

    local_owner_repos = set([tup[0] for tup in ck_client.execute_no_params(uniq_owner_repos_sql)])
    remote_owner_repos = set([tup[0] for tup in ck_client.execute_no_params(remote_uniq_owner_repos_sql)])
    ck_client.close()

    owner_repos = None
    if combination_type == 'union':
        owner_repos = local_owner_repos.union(remote_owner_repos)
    elif combination_type == 'intersection':
        owner_repos = local_owner_repos.intersection(remote_owner_repos)
    elif combination_type == 'diff_local':
        owner_repos = local_owner_repos.difference(remote_owner_repos)
    elif combination_type == 'diff_remote':
        owner_repos = remote_owner_repos.difference(local_owner_repos)
    else:
        raise ValueError(f"Unknown combination type: {combination_type}")

    logger.info(f"Combined owner-repo pairs: {owner_repos}")
    return owner_repos


def sync_from_remote_by_repos(local_ck_conn_info, remote_ck_conn_info, table_name, owner_repos):
    failed_owner_repos = []
    failure_info = {}  # Key: err.code, value: err.message
    for owner_repo_pair in owner_repos:
        owner, repo = owner_repo_pair
        try:
            sync_from_remote_by_repo(local_ck_conn_info, remote_ck_conn_info, table_name, owner, repo)
        except clickhouse_driver.errors.ServerException as e:
            logger.error(f"Failed to sync {owner}/{repo}: {e.code}")
            if e.code not in failure_info:
                failure_info[e.code] = e.message
            failed_owner_repos.append((owner, repo, e.code))

    if failed_owner_repos:
        logger.error(f"Failure messages: {json.dumps(failure_info, indent=2)}")
        raise Exception(f"Failed to sync {len(failed_owner_repos)} repos: {failed_owner_repos}")


def sync_from_remote_by_repo(local_ck_conn_info, remote_ck_conn_info, table_name, owner, repo):
    local_ck_client = CKServer(host=local_ck_conn_info.get("HOST"),
                               port=local_ck_conn_info.get("PORT"),
                               user=local_ck_conn_info.get("USER"),
                               password=local_ck_conn_info.get("PASSWD"),
                               database=local_ck_conn_info.get("DATABASE"),
                               settings={
                                   "max_execution_time": 1000000,
                               },
                               kwargs={
                                   "connect_timeout": 200,
                                   "send_receive_timeout": 6000,
                                   "sync_request_timeout": 100,
                               })
    local_ck_client.execute_no_params(FAIL_OVER_TIMEOUT_SETTING_SQL)

    local_db = local_ck_conn_info.get("DATABASE")

    remote_host = remote_ck_conn_info.get('HOST')
    remote_port = remote_ck_conn_info.get('PORT')
    remote_user = remote_ck_conn_info.get('USER')
    remote_password = remote_ck_conn_info.get('PASSWD')
    remote_db = remote_ck_conn_info.get('DATABASE')

    local_latest_updated_at_sql = f"""
    select search_key__updated_at from {local_db}.{table_name}
    where search_key__owner = '{owner}' and search_key__repo = '{repo}'
    order by search_key__updated_at desc
    limit 1
    """

    rows = local_ck_client.execute_no_params(local_latest_updated_at_sql)
    local_latest_updated_at = 0 if not rows else rows[0][0]
    cols_str = get_table_cols_str(local_ck_client, local_db, table_name)
    insert_sql = f"""
    insert into table {local_db}.{table_name}
    select {cols_str}
    from remote(
            '{remote_host}:{remote_port}',
            '{remote_db}.{table_name}',
            '{remote_user}',
            '{remote_password}'
        )
       where search_key__owner = '{owner}'
       and search_key__repo = '{repo}'
        and search_key__updated_at > {local_latest_updated_at}
    """
    logger.info(f"Syncing {owner}/{repo}(updated_at > {local_latest_updated_at})")
    local_ck_client.execute_no_params(insert_sql)

    # Log for the inserted data
    new_insert_count_sql = f"""
    select count() from {local_db}.{table_name}
    where search_key__owner = '{owner}'
    and search_key__repo = '{repo}'
    and search_key__updated_at > {local_latest_updated_at}
    """
    result = local_ck_client.execute_no_params(new_insert_count_sql)
    new_insert_count = 0 if not result else result[0][0]
    logger.info(
        f"Synced {new_insert_count} rows for {owner}/{repo}(updated_at > {local_latest_updated_at})"
        f"[Might not be accurate cuz of asynchronization]")


def sync_github_profiles_to_ck(os_conn_info, ck_conn_info, github_profile_table_template):
    # Sync OpenSearch github_profile docs, whose search_key.updated_at is greater than the latest
    # search_key__updated_at in ClickHouse, to ClickHouse
    latest_updated_at_sql = 'select max(search_key__updated_at) from github_profile'
    ck_client = CKServer(host=ck_conn_info.get("HOST"),
                         port=ck_conn_info.get("PORT"),
                         user=ck_conn_info.get("USER"),
                         password=ck_conn_info.get("PASSWD"),
                         database=ck_conn_info.get("DATABASE"),
                         settings={
                             "max_execution_time": 1000000,
                         },
                         kwargs={
                             "connect_timeout": 200,
                             "send_receive_timeout": 6000,
                             "sync_request_timeout": 100,
                         })
    result = ck_client.execute_no_params(latest_updated_at_sql)
    latest_updated_at = result[0][0]

    os_client = get_opensearch_client(opensearch_conn_info=os_conn_info)
    query = {
        "query": {
            "range": {
                "search_key.updated_at": {
                    "gt": latest_updated_at,
                }
            }
        }
    }
    latest_updated_at_datetime = datetime.fromtimestamp(latest_updated_at / 1000)
    logger.info(f'Sync os docs whose search_key.updated_at > {latest_updated_at} '
                f'({latest_updated_at_datetime}) to clickhouse')
    opensearch_to_clickhouse(os_client, OPENSEARCH_INDEX_GITHUB_PROFILE, query, ck_client,
                             github_profile_table_template, OPENSEARCH_INDEX_GITHUB_PROFILE)


# TODO There are common logic in function sync_github_profiles_from_remote_ck and sync_from_remote_by_repos
#  Consider extract the common part and define a new abstract function, which:
#  Sync remote clickhouse data into the local, by specifying db name, table name and query condition
def sync_github_profiles_from_remote_ck(local_ck_conn_info, remote_ck_conn_info):
    # Sync github profile from a remote clickhouse service to the local clickhouse service, where the
    # remote data's search_key__updated_at is greater than max local search_key__updated_at
    local_ck_client = CKServer(host=local_ck_conn_info.get("HOST"),
                               port=local_ck_conn_info.get("PORT"),
                               user=local_ck_conn_info.get("USER"),
                               password=local_ck_conn_info.get("PASSWD"),
                               database=local_ck_conn_info.get("DATABASE"),
                               settings={
                                   "max_execution_time": 1000000,
                               },
                               kwargs={
                                   "connect_timeout": 200,
                                   "send_receive_timeout": 6000,
                                   "sync_request_timeout": 100,
                                   # "connect_timeout_with_failover_ms": 20000,
                               })
    local_db = local_ck_conn_info.get("DATABASE")

    remote_host = remote_ck_conn_info.get('HOST')
    remote_port = remote_ck_conn_info.get('PORT')
    remote_user = remote_ck_conn_info.get('USER')
    remote_password = remote_ck_conn_info.get('PASSWD')
    remote_db = remote_ck_conn_info.get('DATABASE')

    local_latest_updated_at_sql = \
        f'select max(search_key__updated_at) from {local_db}.{OPENSEARCH_INDEX_GITHUB_PROFILE}'

    result = local_ck_client.execute_no_params(local_latest_updated_at_sql)[0]
    local_latest_updated_at = result[0] if result else 0

    cols_str = get_table_cols_str(local_ck_client, local_db, OPENSEARCH_INDEX_GITHUB_PROFILE)
    insert_sql = f'''
    insert into table {local_db}.{OPENSEARCH_INDEX_GITHUB_PROFILE}
    select {cols_str}
    from remote(
            '{remote_host}:{remote_port}',
            '{remote_db}.{OPENSEARCH_INDEX_GITHUB_PROFILE}',
            '{remote_user}',
            '{remote_password}'
        )
       where search_key__updated_at > {local_latest_updated_at}
    '''

    logger.info(insert_sql)

    logger.info(f"Syncing github_profile (updated_at > {local_latest_updated_at})")
    local_ck_client.execute_no_params(insert_sql)

    # Log for the inserted data
    new_insert_count_sql = f"""
        select count() from {local_db}.{OPENSEARCH_INDEX_GITHUB_PROFILE}
        where search_key__updated_at > {local_latest_updated_at}
        """
    result = local_ck_client.execute_no_params(new_insert_count_sql)
    new_insert_count = 0 if not result else result[0][0]
    logger.info(f"Synced {new_insert_count} github_profile rows (updated_at > {local_latest_updated_at})")


# A common util to sync data from remote ck
def sync_data_from_remote_ck(local_ck_conn_info, remote_ck_conn_info, table_name, ts_col):
    # Sync github profile from a remote clickhouse service to the local clickhouse service, where the
    # remote data's search_key__updated_at is greater than max local search_key__updated_at
    local_ck_client = CKServer(host=local_ck_conn_info.get("HOST"),
                               port=local_ck_conn_info.get("PORT"),
                               user=local_ck_conn_info.get("USER"),
                               password=local_ck_conn_info.get("PASSWD"),
                               database=local_ck_conn_info.get("DATABASE"),
                               settings={
                                   "max_execution_time": 1000000,
                               },
                               kwargs={
                                   "connect_timeout": 200,
                                   "send_receive_timeout": 6000,
                                   "sync_request_timeout": 100,
                                   # "connect_timeout_with_failover_ms": 20000,
                               })
    local_db = local_ck_conn_info.get("DATABASE")

    remote_host = remote_ck_conn_info.get('HOST')
    remote_port = remote_ck_conn_info.get('PORT')
    remote_user = remote_ck_conn_info.get('USER')
    remote_password = remote_ck_conn_info.get('PASSWD')
    remote_db = remote_ck_conn_info.get('DATABASE')

    local_latest_ts_sql = f'select max({ts_col}) from {local_db}.{table_name}'
    # max() will ensure a list with 1 element
    result = local_ck_client.execute_no_params(local_latest_ts_sql)[0]
    local_latest_updated_at = result[0] if result else 0

    cols_str = get_table_cols_str(local_ck_client, local_db, table_name)
    insert_sql = f'''
    insert into table {local_db}.{table_name}
    select {cols_str}
    from remote(
            '{remote_host}:{remote_port}',
            '{remote_db}.{table_name}',
            '{remote_user}',
            '{remote_password}'
        )
       where {ts_col} > {local_latest_updated_at}
    '''

    logger.info(insert_sql)

    local_ck_client.execute_no_params(FAIL_OVER_TIMEOUT_SETTING_SQL)
    logger.info(f"Syncing {table_name} ({ts_col} > {local_latest_updated_at})")
    local_ck_client.execute_no_params(insert_sql)

    # Log for the inserted data
    new_insert_count_sql = f"""
        select count() from {local_db}.{table_name}
        where {ts_col} > {local_latest_updated_at}
        """
    result = local_ck_client.execute_no_params(new_insert_count_sql)
    new_insert_count = 0 if not result else result[0][0]
    logger.info(f"Synced {new_insert_count} {table_name} rows (updated_at > {local_latest_updated_at})")


def get_table_cols(ck_client, db_name, table_name):
    # Get db_name.db_table's columns from table system.columns
    table_col_names_sql = f"""
            select distinct name
            from system.columns
            where database = '{db_name}'
              and table = '{table_name}'
            """
    cols = ck_client.execute_no_params(table_col_names_sql)

    return cols


def get_table_cols_str(ck_client, db_name, table_name, splitter=','):
    # Quote table columns by ` to avoid SQL error
    # Return the string with the column names joined by splitter
    cols = get_table_cols(ck_client, db_name, table_name)
    return splitter.join([f'`{col[0]}`' for col in cols])
