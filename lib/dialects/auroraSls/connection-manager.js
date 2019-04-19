'use strict';

const AbstractConnectionManager = require('../abstract/connection-manager');
const SequelizeErrors = require('../../errors');
const Promise = require('../../promise');
const { logger } = require('../../utils/logger');
const DataTypes = require('../../data-types').auroraSls;
const momentTz = require('moment-timezone');
const debug = logger.debugContext('connection:mysql');
const parserStore = require('../parserStore')('mysql');

/**
 * MySQL Connection Manager
 *
 * Get connections, validate and disconnect them.
 * AbstractConnectionManager pooling use it to handle MySQL specific connections
 * Use https://github.com/sidorares/node-mysql2 to connect with MySQL server
 *
 * @extends AbstractConnectionManager
 * @returns Class<ConnectionManager>
 * @private
 */

class ConnectionManager extends AbstractConnectionManager {
  constructor(dialect, sequelize) {
    sequelize.config.port = sequelize.config.port || 3306;
    super(dialect, sequelize);
    this.lib = this._loadDialectModule('aws-sdk');
    this.refreshTypeParser(DataTypes);
  }

  _refreshTypeParser(dataType) {
    parserStore.refresh(dataType);
  }

  _clearTypeParser() {
    parserStore.clear();
  }

  static _typecast(field, next) {
    if (parserStore.get(field.type)) {
      return parserStore.get(field.type)(field, this.sequelize.options, next);
    }
    return next();
  }

  /**
   * Connect with MySQL database based on config, Handle any errors in connection
   * Set the pool handlers on connection.error
   * Also set proper timezone once connection is connected.
   *
   * @param {Object} config
   * @returns {Promise<Connection>}
   * @private
   */
  connect(config) {
    const { dialectOptions } = config;
    const rds = new this.lib.RDSDataService(dialectOptions.awsConfig);
    const query = ({ sql }, handler) => {
      rds.executeSql({
        awsSecretStoreArn: dialectOptions.awsSecretStoreArn,
        dbClusterOrInstanceArn: dialectOptions.dbClusterOrInstanceArn,
        sqlStatements: sql,
        database: dialectOptions.database,
        schema: dialectOptions.schema
      }, handler);
    };
    const connection = { query, execute: query };
    return new Promise(resolve => {
      connection.query(
        { sql: 'SELECT VERSION() as `version`' },
        err => {
          if (err) {
            // @TODO better error handling
            switch (err.code) {
              case 'ECONNREFUSED':
                throw new SequelizeErrors.ConnectionRefusedError(err);
              case 'ER_ACCESS_DENIED_ERROR':
                throw new SequelizeErrors.AccessDeniedError(err);
              case 'ENOTFOUND':
                throw new SequelizeErrors.HostNotFoundError(err);
              case 'EHOSTUNREACH':
                throw new SequelizeErrors.HostNotReachableError(err);
              case 'EINVAL':
                throw new SequelizeErrors.InvalidConnectionError(err);
              default:
                throw new SequelizeErrors.ConnectionError(err);
            }
          } else {
            resolve(connection);
          }
        });
    });
  }

  disconnect() {
    return Promise.resolve();
  }

  validate(connection) {
    return connection;
  }
}

module.exports = ConnectionManager;
module.exports.ConnectionManager = ConnectionManager;
module.exports.default = ConnectionManager;
