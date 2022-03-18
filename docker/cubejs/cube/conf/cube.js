const ClickhouseDriver = require('@cubejs-backend/clickhouse-driver');
const PostgresDriver = require('@cubejs-backend/postgres-driver');

module.exports = {
    logger: (msg, params) => {
        console.log(`${msg}: ${JSON.stringify(params)}`);
    },
    dbType: ({dataSource} = {}) => {
        if (dataSource === 'Clickhouse') {
            return 'clickhouse';
        } else {
            return 'clickhouse';
        }
    },
    driverFactory: ({dataSource} = {}) => {
        if (dataSource === 'Clickhouse') {
            return new ClickhouseDriver({
                database: 'default',
                host: '192.168.8.108',
                user: 'default',
                password: 'default',
                port: 18000,
            });
        } else {
            return new ClickhouseDriver({
                database: 'default',
                host: '192.168.8.108',
                user: 'default',
                password: 'default',
                port: 18000,
            });
        }
    },
};