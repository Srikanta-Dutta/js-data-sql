var babel = require('rollup-plugin-babel')

module.exports = {
  external: [
    'knex',
    'bluebird',
    'js-data',
    'js-data-adapter',
    'mout/lang/toString',
    'mout/string/underscore'
  ],
  plugins: [
    babel({
      exclude: 'node_modules/**'
    })
  ]
}
