import random
import requests
import time
import itertools

from opensearchpy import OpenSearch

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.base_dict.options import GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX


class SyncGithubIssuesCommentsException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


def sync_github_issues_comments(opensearch_conn_info,
                                owner,
                                repo,
                                token_proxy_accommodator,
                                issues_numbers):
    logger.info(f'Sync comments of {owner}/{repo} on issues: {issues_numbers}')
    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_info["HOST"], 'port': opensearch_conn_info["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_info["USER"], opensearch_conn_info["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )

    session = requests.Session()
    opensearch_api = OpensearchAPI()
    github_api = GithubAPI()
    for issues_number in issues_numbers:
        del_result = opensearch_client.delete_by_query(index=OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS,
                                                       body={
                                                           "track_total_hits": True,
                                                           "query": {
                                                               "bool": {
                                                                   "must": [
                                                                       {
                                                                           "term": {
                                                                               "search_key.owner.keyword": {
                                                                                   "value": owner
                                                                               }
                                                                           }
                                                                       },
                                                                       {
                                                                           "term": {
                                                                               "search_key.repo.keyword": {
                                                                                   "value": repo
                                                                               }
                                                                           }
                                                                       },
                                                                       {
                                                                           "term": {
                                                                               "search_key.number": {
                                                                                   "value": issues_number
                                                                               }
                                                                           }
                                                                       }
                                                                   ]
                                                               }
                                                           }
                                                       })
        logger.info(f"DELETE github issues {issues_number} comments result:{del_result}")

        page = 1
        while True:
            # Token sleep
            time.sleep(random.uniform(GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX))

            req = github_api.get_github_issues_comments(
                http_session=session,
                token_proxy_accommodator=token_proxy_accommodator,
                owner=owner,
                repo=repo,
                number=issues_number,
                page=page
            )
            one_page_github_issues_comments = req.json()

            if not one_page_github_issues_comments:
                logger.info(f"sync github issues comments end to break:{owner}/{repo} page_index:{page}")
                break

            opensearch_api.bulk_github_issues_comments(opensearch_client=opensearch_client,
                                                       issues_comments=one_page_github_issues_comments,
                                                       owner=owner, repo=repo, number=issues_number, if_sync=1)

            logger.info(f"success get github issues comments page:{owner}/{repo} page_index:{page}")
            page += 1

    # 建立 sync 标志
    # opensearch_api.set_sync_github_issues_check(opensearch_client, owner, repo)
