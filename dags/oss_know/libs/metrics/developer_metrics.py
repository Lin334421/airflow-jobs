import networkx as nx
from clickhouse_driver.errors import ServerException as ClickHouseServerException
from networkx.exception import PowerIterationFailedConvergence

from oss_know.libs.metrics.influence_metrics import MetricRoutineCalculation
from oss_know.libs.util.log import logger


class PrivilegeEventsMetricRoutineCalculation(MetricRoutineCalculation):
    privileged_events_list = ["added_to_project", "converted_note_to_issue",
                              "deployed", "deployment_environment_changed",
                              "locked", "merged", "moved_columns_in_project",
                              "pinned", "removed_from_project",
                              "review_dismissed", "transferred",
                              "unlocked", "unpinned", "user_blocked"]

    def calculate_metrics(self):
        logger.info(f'Calculating {self.owner}/{self.repo} privileged event metrics')
        event_type_index_map = {}
        for (index, event) in enumerate(self.privileged_events_list):
            event_type_index_map[event] = index

        privilege_sql_ = f"""
        WITH {PrivilegeEventsMetricRoutineCalculation.privileged_events_list} as privileged_events
        SELECT search_key__owner,
               search_key__repo,
               toYYYYMM(created_at)                                                   as year_month,
               JSONExtractString(JSONExtractString(timeline_raw, 'actor'), 'login') as _actor_login,
               groupArray(distinct search_key__event)                                 as the_privileged_events
        FROM github_issues_timeline
        WHERE search_key__owner = '{self.owner}'
          AND search_key__repo = '{self.repo}'
          and has(privileged_events, search_key__event)
          and _actor_login != ''
        group by search_key__owner, search_key__repo, year_month, _actor_login;
        """

        # TODO Even with constraints on search_key__event, an iterator is essential when data expands
        #  to a large scale.
        privilege_results = self.clickhouse_client.execute_no_params(privilege_sql_)
        logger.info(f'{len(privilege_results)} Privilege Events Metrics calculated on {self.owner}/{self.repo}')
        return privilege_results

    def save_metrics(self):
        logger.info(f'Saving Privilege Events Metrics of {self.owner}/{self.repo}')
        self.clickhouse_client.execute(f'INSERT INTO {self.table_name} VALUES ', self.batch)


class CountMetricRoutineCalculation(MetricRoutineCalculation):
    def calculate_metrics(self):
        # TODO Add and handle author_email
        gits_sql_ = f"""
        SELECT
        toValidUTF8(substring(author_name, 1, 256)) as author_name,
        count() AS commit_count,
        sum(total__lines) AS total_lines
        FROM gits
        WHERE search_key__owner = '{self.owner}'
          AND search_key__repo = '{self.repo}'
        GROUP BY author_name
        """
        gits_results = self.clickhouse_client.execute_no_params(gits_sql_)

        logger.info(f'Calculating Count Metrics of {self.owner}/{self.repo}')
        return gits_results

    def save_metrics(self):
        count_metrics_insert_query = f'''
            INSERT INTO {self.table_name}
            (search_key__owner, search_key__repo, author_name, commit_num, line_of_code)
            VALUES ("{self.owner}", "{self.repo}", %s, %s, %s)'''

        self.batch_insertion(insert_query=count_metrics_insert_query, batch=self.batch)
        logger.info(f'{len(self.batch)} Count Metrics saved for {self.owner}/{self.repo}')


class NetworkMetricRoutineCalculation(MetricRoutineCalculation):
    def files_first_chars(self):
        first_chars_sql = f'''
        SELECT DISTINCT substring(file__name, 1, 1) AS first_char
        FROM (
                 SELECT `files.file_name` AS file__name
                 FROM gits
                          ARRAY JOIN `files.file_name`
                 WHERE search_key__owner = '{self.owner}'
                   AND search_key__repo = '{self.repo}'
                 GROUP BY file__name)
        WHERE first_char != '';
        '''
        result = self.clickhouse_client.execute_no_params(first_chars_sql)
        return [tup[0] for tup in result]

    def developer_relation_sql(self, group_char, day_threshold=180):
        return f'''
        WITH '{self.owner}' AS owner, '{self.repo}' AS repo, '{group_char}' AS first_char
        SELECT a_author_name, b_author_name
        FROM (SELECT a.author_name AS a_author_name,
                     a.day         AS a_day,
                     b.author_name AS b_author_name,
                     b.day         AS b_day
              FROM (SELECT author_name, toYYYYMMDD(authored_date) AS day, `files.file_name` AS file_name
                    FROM gits
                             ARRAY JOIN `files.file_name`
                    WHERE search_key__owner = owner
                      AND search_key__repo = repo
                      AND length(parents) == 1
                      AND substring(file_name, 1, 1) = first_char
                    GROUP BY author_name, day, file_name
                    ORDER BY file_name, day) AS a GLOBAL
                       JOIN (SELECT author_name, toYYYYMMDD(authored_date) AS day, `files.file_name` AS file_name
                             FROM gits
                                      ARRAY JOIN `files.file_name`
                             WHERE search_key__owner = owner
                               AND search_key__repo = repo
                               AND length(parents) == 1
                               AND substring(file_name, 1, 1) = first_char
                             GROUP BY author_name, day, file_name
                             ORDER BY file_name, day) AS b ON a.file_name = b.file_name)
        WHERE a_author_name != b_author_name
          AND abs(a_day - b_day) <= {day_threshold}
        GROUP BY a_author_name, b_author_name
        '''

    def calculate_metrics(self):
        logger.info(f'Calculate {self.owner}/{self.repo} developer network metrics')
        first_chars = self.files_first_chars()

        social_network = nx.Graph()
        for first_char in first_chars:
            developer_relations_sql = self.developer_relation_sql(first_char)
            try:
                developer_pairs = self.clickhouse_client.execute_no_params(developer_relations_sql)
            except ClickHouseServerException as e:
                logger.error(
                    f'Failed to calculate developer pairs for {self.owner}/{self.repo},'
                    f' group {first_char}: {e}'
                )
                developer_pairs = []

            logger.info(f'{len(developer_pairs)} pairs on group {first_char}, {self.owner}/{self.repo}')
            for (dev1, dev2) in developer_pairs:
                social_network.add_edge(dev1, dev2)

        if nx.is_empty(social_network):
            logger.warning(f'No developer pair edges found for {self.owner}/{self.repo}, skip')
            return []

        try:
            eigenvector = nx.eigenvector_centrality(social_network, max_iter=3000)
        except PowerIterationFailedConvergence:
            logger.error(f'Failed to calculate eigenvector_centrality even with 3000 iterations'
                         f' for {self.owner}/{self.repo}')
            eigenvector = {}

        response = []
        dev_nodes = list(social_network.nodes())
        for dev in dev_nodes:
            degree = social_network.degree(dev)
            response.append((self.owner, self.repo, dev, degree, eigenvector.get(dev) or 0))

        node_num = social_network.number_of_nodes()
        edge_num = social_network.number_of_edges()
        log = f'Network Metrics of {self.owner}/{self.repo} calculated, {node_num} nodes and ' \
              f'{edge_num} edges in the graph'
        logger.info(log)

        return response

    def save_metrics(self):
        logger.info('saving  Network Metrics')
        network_metrics_insert_query = f'''
            INSERT INTO {self.table_name}
            (search_key__owner, search_key__repo,
            author_name, degree_centrality, eigenvector_centrality)
            VALUES (%s, %s, %s, %s, %s)'''
        self.batch_insertion(insert_query=network_metrics_insert_query, batch=self.batch)
