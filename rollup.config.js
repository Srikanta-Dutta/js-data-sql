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
      babelrc: false,
      presets: [
        'es2015-rollup'
      ],
      exclude: 'node_modules/**'
    })
  ]
}