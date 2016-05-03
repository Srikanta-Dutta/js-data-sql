/*global assert:true */
'use strict'

// prepare environment for js-data-adapter-tests
require('babel-polyfill')

var JSData = require('js-data')
var JSDataAdapterTests = require('js-data-adapter-tests')
var JSDataSql = require('./')

var assert = global.assert = JSDataAdapterTests.assert
global.sinon = JSDataAdapterTests.sinon

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

JSDataAdapterTests.init({
  debug: false,
  JSData: JSData,
  Adapter: JSDataSql.SqlAdapter,
  adapterConfig: {
    knexOptions: {
      client: DB_CLIENT,
      connection: connection,
      pool: {
        min: 0,
        max: 10
      },
      debug: !!process.env.DEBUG
    },
    debug: !!process.env.DEBUG
  },
  // js-data-sql does NOT support these features
  xfeatures: [
    'findHasManyLocalKeys',
    'findHasManyForeignKeys',
    'filterOnRelations'
  ]
})

describe('exports', function () {
  it('should have correct exports', function () {
    assert(JSDataSql.default)
    assert(JSDataSql.SqlAdapter)
    assert(JSDataSql.SqlAdapter === JSDataSql.default)
    assert(JSDataSql.version)
    assert(JSDataSql.version.full)
  })
})

require('./test/create_trx.spec')
require('./test/destroy_trx.spec')
require('./test/filterQuery.spec')
require('./test/update_trx.spec')
