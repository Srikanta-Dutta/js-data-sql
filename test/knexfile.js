var DB_CLIENT = process.env.DB_CLIENT || 'mysql'

var connection

if (DB_CLIENT === 'sqlite3') {
  connection = {
    filename: process.env.DB_FILE
  }
} else {
  connection = {
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER || process.env.C9_USER || 'ubuntu',
    database: process.env.DB_NAME || (process.env.C9_USER ? 'c9' : 'circle_test')
  }
}

var config = {
  client: DB_CLIENT,
  connection: connection,
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