var DB_CLIENT = process.env.DB_CLIENT || 'mysql'

var connection

if (DB_CLIENT === 'sqlite3') {
  connection = {
    filename: process.env.DB_FILE
  }
} else {
  connection = {
    host: process.env.DB_HOST || '127.0.0.1',
    user: process.env.DB_USER,
    database: process.env.DB_NAME
  }
}

var config = {
  client: DB_CLIENT,
  connection: connection,
  pool: {
    min: 1,
    max: 10
  },
  migrations: {
    tableName: 'migrations'
  },
  debug: !!process.env.DEBUG
}

// Workaround for knex not playing well with nconf
if (process.env.NODE_ENV) {
  config = {}
  config[process.env.NODE_ENV] = config
}

module.exports = config