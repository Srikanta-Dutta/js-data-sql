'use strict';

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

var knex = _interopDefault(require('knex'));
var Promise = _interopDefault(require('bluebird'));
var jsData = require('js-data');
var jsDataAdapter = require('js-data-adapter');
var toString = _interopDefault(require('mout/lang/toString'));
var underscore = _interopDefault(require('mout/string/underscore'));

var babelHelpers = {};

babelHelpers.slicedToArray = function () {
  function sliceIterator(arr, i) {
    var _arr = [];
    var _n = true;
    var _d = false;
    var _e = undefined;

    try {
      for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) {
        _arr.push(_s.value);

        if (i && _arr.length === i) break;
      }
    } catch (err) {
      _d = true;
      _e = err;
    } finally {
      try {
        if (!_n && _i["return"]) _i["return"]();
      } finally {
        if (_d) throw _e;
      }
    }

    return _arr;
  }

  return function (arr, i) {
    if (Array.isArray(arr)) {
      return arr;
    } else if (Symbol.iterator in Object(arr)) {
      return sliceIterator(arr, i);
    } else {
      throw new TypeError("Invalid attempt to destructure non-iterable instance");
    }
  };
}();

babelHelpers;

jsData.utils.Promise = Promise;

var DEFAULTS = {};

var equal = function equal(query, field, value, isOr) {
  if (value === null) {
    return query[isOr ? 'orWhereNull' : 'whereNull'](field);
  }
  return query[getWhereType(isOr)](field, value);
};

var notEqual = function notEqual(query, field, value, isOr) {
  if (value === null) {
    return query[isOr ? 'orWhereNotNull' : 'whereNotNull'](field);
  }
  return query[getWhereType(isOr)](field, '!=', value);
};

var getWhereType = function getWhereType(isOr) {
  return isOr ? 'orWhere' : 'where';
};

var MILES_REGEXP = /(\d+(\.\d+)?)\s*(m|M)iles$/;
var KILOMETERS_REGEXP = /(\d+(\.\d+)?)\s*(k|K)$/;

/**
 * Default predicate functions for the filtering operators.
 *
 * @name module:js-data-sql.OPERATORS
 * @property {Function} == Equality operator.
 * @property {Function} != Inequality operator.
 * @property {Function} > "Greater than" operator.
 * @property {Function} >= "Greater than or equal to" operator.
 * @property {Function} < "Less than" operator.
 * @property {Function} <= "Less than or equal to" operator.
 * @property {Function} isectEmpty Operator to test that the intersection
 * between two arrays is empty. Not supported.
 * @property {Function} isectNotEmpty Operator to test that the intersection
 * between two arrays is NOT empty. Not supported.
 * @property {Function} in Operator to test whether a value is found in the
 * provided array.
 * @property {Function} notIn Operator to test whether a value is NOT found in
 * the provided array.
 * @property {Function} contains Operator to test whether an array contains the
 * provided value. Not supported.
 * @property {Function} notContains Operator to test whether an array does NOT
 * contain the provided value. Not supported.
 */
var OPERATORS = {
  '==': equal,
  '===': equal,
  '!=': notEqual,
  '!==': notEqual,
  '>': function _(query, field, value, isOr) {
    return query[getWhereType(isOr)](field, '>', value);
  },
  '>=': function _(query, field, value, isOr) {
    return query[getWhereType(isOr)](field, '>=', value);
  },
  '<': function _(query, field, value, isOr) {
    return query[getWhereType(isOr)](field, '<', value);
  },
  '<=': function _(query, field, value, isOr) {
    return query[getWhereType(isOr)](field, '<=', value);
  },
  'isectEmpty': function isectEmpty(query, field, value, isOr) {
    throw new Error('isectEmpty not supported!');
  },
  'isectNotEmpty': function isectNotEmpty(query, field, value, isOr) {
    throw new Error('isectNotEmpty not supported!');
  },
  'in': function _in(query, field, value, isOr) {
    return query[getWhereType(isOr)](field, 'in', value);
  },
  'notIn': function notIn(query, field, value, isOr) {
    return query[isOr ? 'orNotIn' : 'notIn'](field, value);
  },
  'contains': function contains(query, field, value, isOr) {
    throw new Error('contains not supported!');
  },
  'notContains': function notContains(query, field, value, isOr) {
    throw new Error('notContains not supported!');
  },
  'like': function like(query, field, value, isOr) {
    return query[getWhereType(isOr)](field, 'like', value);
  },
  'near': function near(query, field, value, isOr) {
    var radius = void 0;
    var unitsPerDegree = void 0;
    if (typeof value.radius === 'number' || MILES_REGEXP.test(value.radius)) {
      radius = typeof value.radius === 'number' ? value.radius : value.radius.match(MILES_REGEXP)[1];
      unitsPerDegree = 69.0; // miles per degree
    } else if (KILOMETERS_REGEXP.test(value.radius)) {
        radius = value.radius.match(KILOMETERS_REGEXP)[1];
        unitsPerDegree = 111.045; // kilometers per degree;
      } else {
          throw new Error('Unknown radius distance units');
        }

    var _field$split$map = field.split(',').map(function (c) {
      return c.trim();
    });

    var _field$split$map2 = babelHelpers.slicedToArray(_field$split$map, 2);

    var latitudeColumn = _field$split$map2[0];
    var longitudeColumn = _field$split$map2[1];

    var _value$center = babelHelpers.slicedToArray(value.center, 2);

    var latitude = _value$center[0];
    var longitude = _value$center[1];

    // Uses indexes on `latitudeColumn` / `longitudeColumn` if available

    query = query.whereBetween(latitudeColumn, [latitude - radius / unitsPerDegree, latitude + radius / unitsPerDegree]).whereBetween(longitudeColumn, [longitude - radius / (unitsPerDegree * Math.cos(latitude * (Math.PI / 180))), longitude + radius / (unitsPerDegree * Math.cos(latitude * (Math.PI / 180)))]);

    if (value.calculateDistance) {
      var distanceColumn = typeof value.calculateDistance === 'string' ? value.calculateDistance : 'distance';
      query = query.select(knex.raw('\n        ' + unitsPerDegree + ' * DEGREES(ACOS(\n          COS(RADIANS(?)) * COS(RADIANS(' + latitudeColumn + ')) *\n          COS(RADIANS(' + longitudeColumn + ') - RADIANS(?)) +\n          SIN(RADIANS(?)) * SIN(RADIANS(' + latitudeColumn + '))\n        )) AS ' + distanceColumn, [latitude, longitude, latitude]));
    }
    return query;
  }
};

Object.freeze(OPERATORS);

/**
 * SqlAdapter class.
 *
 * @example
 * // Use Container instead of DataStore on the server
 * import {Container} from 'js-data'
 * import SqlAdapter from 'js-data-sql'
 *
 * // Create a store to hold your Mappers
 * const store = new Container()
 *
 * // Create an instance of SqlAdapter with default settings
 * const adapter = new SqlAdapter()
 *
 * // Mappers in "store" will use the Sql adapter by default
 * store.registerAdapter('sql', adapter, { default: true })
 *
 * // Create a Mapper that maps to a "user" table
 * store.defineMapper('user')
 *
 * @class SqlAdapter
 * @extends Adapter
 * @param {Object} [opts] Configuration options.
 * @param {boolean} [opts.debug=false] See {@link Adapter#debug}.
 * @param {Object} [opts.operators] See {@link SqlAdapter#operators}.
 * @param {boolean} [opts.raw=false] See {@link Adapter#raw}.
 */
function SqlAdapter(opts) {
  var self = this;
  jsData.utils.classCallCheck(self, SqlAdapter);
  opts || (opts = {});
  opts.knexOptions || (opts.knexOptions = {});
  jsData.utils.fillIn(opts, DEFAULTS);

  Object.defineProperty(self, 'knex', {
    writable: true,
    value: undefined
  });

  jsDataAdapter.Adapter.call(self, opts);

  self.knex || (self.knex = knex(self.knexOptions));

  /**
   * Override the default predicate functions for specified operators.
   *
   * @name SqlAdapter#operators
   * @type {Object}
   * @default {}
   */
  self.operators || (self.operators = {});

  jsData.utils.fillIn(self.operators, OPERATORS);
}

// Setup prototype inheritance from Adapter
SqlAdapter.prototype = Object.create(jsDataAdapter.Adapter.prototype, {
  constructor: {
    value: SqlAdapter,
    enumerable: false,
    writable: true,
    configurable: true
  }
});

Object.defineProperty(SqlAdapter, '__super__', {
  configurable: true,
  value: jsDataAdapter.Adapter
});

/**
 * Alternative to ES2015 class syntax for extending `SqlAdapter`.
 *
 * @example <caption>Using the ES2015 class syntax.</caption>
 * class MySqlAdapter extends SqlAdapter {...}
 * const adapter = new MySqlAdapter()
 *
 * @example <caption>Using {@link SqlAdapter.extend}.</caption>
 * var instanceProps = {...}
 * var classProps = {...}
 *
 * var MySqlAdapter = SqlAdapter.extend(instanceProps, classProps)
 * var adapter = new MySqlAdapter()
 *
 * @method SqlAdapter.extend
 * @static
 * @param {Object} [instanceProps] Properties that will be added to the
 * prototype of the subclass.
 * @param {Object} [classProps] Properties that will be added as static
 * properties to the subclass itself.
 * @return {Constructor} Subclass of `SqlAdapter`.
 */
SqlAdapter.extend = jsData.utils.extend;

function getTable(mapper) {
  return mapper.table || underscore(mapper.name);
}

/*
function processRelationField (resourceConfig, query, field, criteria, options, joinedTables) {
  let fieldParts = field.split('.')
  let localResourceConfig = resourceConfig
  let relationPath = []
  let relationName = null;

  while (fieldParts.length >= 2) {
    relationName = fieldParts.shift()
    let [relation] = localResourceConfig.relationList.filter(r => r.relation === relationName || r.localField === relationName)

    if (relation) {
      let relationResourceConfig = resourceConfig.getResource(relation.relation)
      relationPath.push(relation.relation)

      if (relation.type === 'belongsTo' || relation.type === 'hasOne') {
        // Apply table join for belongsTo/hasOne property (if not done already)
        if (!joinedTables.some(t => t === relationPath.join('.'))) {
          let table = getTable(localResourceConfig)
          let localId = `${table}.${relation.localKey}`

          let relationTable = getTable(relationResourceConfig)
          let foreignId = `${relationTable}.${relationResourceConfig.idAttribute}`

          query.leftJoin(relationTable, localId, foreignId)
          joinedTables.push(relationPath.join('.'))
        }
      } else if (relation.type === 'hasMany') {
        // Perform `WHERE EXISTS` subquery for hasMany property
        let existsParams = {
          [`${relationName}.${fieldParts.splice(0).join('.')}`]: criteria // remaining field(s) handled by EXISTS subquery
        };
        let subQueryTable = getTable(relationResourceConfig);
        let subQueryOptions = deepMixIn({}, options, { query: knex(this.defaults).select(`${subQueryTable}.*`).from(subQueryTable) })
        let subQuery = this.filterQuery(relationResourceConfig, existsParams, subQueryOptions)
          .whereRaw('??.??=??.??', [
            getTable(relationResourceConfig),
            relation.foreignKey,
            getTable(localResourceConfig),
            localResourceConfig.idAttribute
          ])
        if (Object.keys(criteria).some(k => k.indexOf('|') > -1)) {
          query.orWhereExists(subQuery);
        } else {
          query.whereExists(subQuery);
        }
      }

      localResourceConfig = relationResourceConfig
    } else {
      // hopefully a qualified local column
    }
  }
  relationName = fieldParts.shift();

  return relationName ? `${getTable(localResourceConfig)}.${relationName}` : null;
}

function loadWithRelations (items, resourceConfig, options) {
  let tasks = []
  let instance = Array.isArray(items) ? null : items

  if (resourceConfig.relationList) {
    resourceConfig.relationList.forEach(def => {
      let relationName = def.relation
      let relationDef = resourceConfig.getResource(relationName)

      let containedName = null
      if (contains(options.with, relationName)) {
        containedName = relationName
      } else if (contains(options.with, def.localField)) {
        containedName = def.localField
      } else {
        return
      }

      let __options = deepMixIn({}, options.orig ? options.orig() : options)

      // Filter to only properties under current relation
      __options.with = options.with.filter(relation => {
        return relation !== containedName &&
        relation.indexOf(containedName) === 0 &&
        relation.length >= containedName.length &&
        relation[containedName.length] === '.'
      }).map(relation => relation.substr(containedName.length + 1))

      let task

      if ((def.type === 'hasOne' || def.type === 'hasMany') && def.foreignKey) {
        task = this.findAll(resourceConfig.getResource(relationName), {
          where: {
            [def.foreignKey]: instance ?
              { '==': instance[def.localKey || resourceConfig.idAttribute] } :
              { 'in': items.map(item => item[def.localKey || resourceConfig.idAttribute]) }
          }
        }, __options).then(relatedItems => {
          if (instance) {
            if (def.type === 'hasOne' && relatedItems.length) {
              instance[def.localField] = relatedItems[0]
            } else {
              instance[def.localField] = relatedItems
            }
          } else {
            items.forEach(item => {
              let attached = relatedItems.filter(ri => ri[def.foreignKey] === item[def.localKey || resourceConfig.idAttribute])
              if (def.type === 'hasOne' && attached.length) {
                item[def.localField] = attached[0]
              } else {
                item[def.localField] = attached
              }
            })
          }

          return relatedItems
        })
      } else if (def.type === 'hasMany' && def.localKeys) {
        // TODO: Write test for with: hasMany property with localKeys
        let localKeys = []

        if (instance) {
          let itemKeys = instance[def.localKeys] || []
          itemKeys = Array.isArray(itemKeys) ? itemKeys : Object.keys(itemKeys)
          localKeys = localKeys.concat(itemKeys || [])
        } else {
          items.forEach(item => {
            let itemKeys = item[def.localKeys] || []
            itemKeys = Array.isArray(itemKeys) ? itemKeys : Object.keys(itemKeys)
            localKeys = localKeys.concat(itemKeys || [])
          })
        }

        task = this.findAll(resourceConfig.getResource(relationName), {
          where: {
            [relationDef.idAttribute]: {
              'in': filter(unique(localKeys), x => x)
            }
          }
        }, __options).then(relatedItems => {
          if (instance) {
            instance[def.localField] = relatedItems
          } else {
            items.forEach(item => {
              let itemKeys = item[def.localKeys] || []
              let attached = relatedItems.filter(ri => itemKeys && contains(itemKeys, ri[relationDef.idAttribute]))
              item[def.localField] = attached
            })
          }

          return relatedItems
        })
      } else if (def.type === 'belongsTo' || (def.type === 'hasOne' && def.localKey)) {
        if (instance) {
          let id = get(instance, def.localKey)
          if (id) {
            task = this.findAll(resourceConfig.getResource(relationName), {
              where: {
                [def.foreignKey || relationDef.idAttribute]: { '==': id }
              }
            }, __options).then(relatedItems => {
              let relatedItem = relatedItems && relatedItems[0];
              instance[def.localField] = relatedItem
              return relatedItem
            })
          }
        } else {
          let ids = items.map(item => get(item, def.localKey)).filter(x => x)
          if (ids.length) {
            task = this.findAll(resourceConfig.getResource(relationName), {
              where: {
                [def.foreignKey || relationDef.idAttribute]: { 'in': ids }
              }
            }, __options).then(relatedItems => {
              items.forEach(item => {
                relatedItems.forEach(relatedItem => {
                  if (relatedItem[def.foreignKey || relationDef.idAttribute] === item[def.localKey]) {
                    item[def.localField] = relatedItem
                  }
                })
              })
              return relatedItems
            })
          }
        }
      }

      if (task) {
        tasks.push(task)
      }
    })
  }
  return Promise.all(tasks)
}
*/

jsData.utils.addHiddenPropsToTarget(SqlAdapter.prototype, {
  _count: function _count(mapper, query, opts) {
    var self = this;
    opts || (opts = {});
    query || (query = {});

    var sqlBuilder = jsData.utils.isUndefined(opts.transaction) ? self.knex : opts.transaction;
    return self.filterQuery(sqlBuilder(getTable(mapper)), query, opts).count('* as count').then(function (rows) {
      return [rows[0].count, {}];
    });
  },
  _create: function _create(mapper, props, opts) {
    var self = this;
    var idAttribute = mapper.idAttribute;
    props || (props = {});
    opts || (opts = {});

    var sqlBuilder = jsData.utils.isUndefined(opts.transaction) ? self.knex : opts.transaction;
    return sqlBuilder(getTable(mapper)).insert(props, idAttribute).then(function (ids) {
      var id = jsData.utils.isUndefined(props[idAttribute]) ? ids.length ? ids[0] : undefined : props[idAttribute];
      if (jsData.utils.isUndefined(id)) {
        throw new Error('Failed to create!');
      }
      return self._find(mapper, id, opts).then(function (result) {
        return [result[0], { ids: ids }];
      });
    });
  },
  _createMany: function _createMany(mapper, props, opts) {
    var self = this;
    props || (props = {});
    opts || (opts = {});

    return Promise.all(props.map(function (record) {
      return self._create(mapper, record, opts);
    })).then(function (results) {
      return [results.map(function (result) {
        return result[0];
      }), {}];
    });
  },
  _destroy: function _destroy(mapper, id, opts) {
    var self = this;
    opts || (opts = {});

    var sqlBuilder = jsData.utils.isUndefined(opts.transaction) ? self.knex : opts.transaction;
    return sqlBuilder(getTable(mapper)).where(mapper.idAttribute, toString(id)).del().then(function () {
      return [undefined, {}];
    });
  },
  _destroyAll: function _destroyAll(mapper, query, opts) {
    var self = this;
    query || (query = {});
    opts || (opts = {});

    var sqlBuilder = jsData.utils.isUndefined(opts.transaction) ? self.knex : opts.transaction;
    return self.filterQuery(sqlBuilder(getTable(mapper)), query, opts).del().then(function () {
      return [undefined, {}];
    });
  },
  _find: function _find(mapper, id, opts) {
    var self = this;
    opts || (opts = {});

    var sqlBuilder = jsData.utils.isUndefined(opts.transaction) ? self.knex : opts.transaction;
    var table = getTable(mapper);
    return sqlBuilder.select(table + '.*').from(table).where(table + '.' + mapper.idAttribute, toString(id)).then(function (rows) {
      if (!rows || !rows.length) {
        return [undefined, {}];
      }
      return [rows[0], {}];
    });
  },
  _findAll: function _findAll(mapper, query, opts) {
    var self = this;
    query || (query = {});
    opts || (opts = {});

    return self.filterQuery(self.selectTable(mapper, opts), query, opts).then(function (rows) {
      return [rows || [], {}];
    });
  },
  _sum: function _sum(mapper, field, query, opts) {
    var self = this;
    if (!jsData.utils.isString(field)) {
      throw new Error('field must be a string!');
    }
    opts || (opts = {});
    query || (query = {});

    var sqlBuilder = jsData.utils.isUndefined(opts.transaction) ? self.knex : opts.transaction;
    return self.filterQuery(sqlBuilder(getTable(mapper)), query, opts).sum(field + ' as sum').then(function (rows) {
      return [rows[0].sum || 0, {}];
    });
  },
  _update: function _update(mapper, id, props, opts) {
    var self = this;
    props || (props = {});
    opts || (opts = {});

    var sqlBuilder = jsData.utils.isUndefined(opts.transaction) ? self.knex : opts.transaction;
    return sqlBuilder(getTable(mapper)).where(mapper.idAttribute, toString(id)).update(props).then(function () {
      return self._find(mapper, id, opts);
    }).then(function (result) {
      if (!result[0]) {
        throw new Error('Not Found');
      }
      return result;
    });
  },
  _updateAll: function _updateAll(mapper, props, query, opts) {
    var self = this;
    var idAttribute = mapper.idAttribute;
    props || (props = {});
    query || (query = {});
    opts || (opts = {});

    var ids = void 0;

    return self._findAll(mapper, query, opts).then(function (result) {
      var records = result[0];
      ids = records.map(function (record) {
        return record[idAttribute];
      });
      var sqlBuilder = jsData.utils.isUndefined(opts.transaction) ? self.knex : opts.transaction;
      return self.filterQuery(sqlBuilder(getTable(mapper)), query, opts).update(props);
    }).then(function () {
      var _query = { where: {} };
      _query.where[idAttribute] = { 'in': ids };
      return self._findAll(mapper, _query, opts);
    });
  },
  _updateMany: function _updateMany(mapper, records, opts) {
    var self = this;
    var idAttribute = mapper.idAttribute;
    records || (records = []);
    opts || (opts = {});

    return Promise.all(records.map(function (record) {
      return self._update(mapper, record[idAttribute], record, opts);
    })).then(function (results) {
      return [results.map(function (result) {
        return result[0];
      }), {}];
    });
  },
  filterQuery: function filterQuery(sqlBuilder, query, opts) {
    var self = this;

    query = jsData.utils.plainCopy(query || {});
    opts || (opts = {});
    opts.operators || (opts.operators = {});
    query.where || (query.where = {});
    query.orderBy || (query.orderBy = query.sort);
    query.orderBy || (query.orderBy = []);
    query.skip || (query.skip = query.offset);

    // Transform non-keyword properties to "where" clause configuration
    jsData.utils.forOwn(query, function (config, keyword) {
      if (jsDataAdapter.reserved.indexOf(keyword) === -1) {
        if (jsData.utils.isObject(config)) {
          query.where[keyword] = config;
        } else {
          query.where[keyword] = {
            '==': config
          };
        }
        delete query[keyword];
      }
    });

    // Filter
    if (Object.keys(query.where).length !== 0) {
      // Apply filter for each field
      jsData.utils.forOwn(query.where, function (criteria, field) {
        if (!jsData.utils.isObject(criteria)) {
          criteria = { '==': criteria };
        }
        // Apply filter for each operator
        jsData.utils.forOwn(criteria, function (value, operator) {
          var isOr = false;
          if (operator && operator[0] === '|') {
            operator = operator.substr(1);
            isOr = true;
          }
          var predicateFn = self.getOperator(operator, opts);
          if (predicateFn) {
            sqlBuilder = predicateFn(sqlBuilder, field, value, isOr);
          } else {
            throw new Error('Operator ' + operator + ' not supported!');
          }
        });
      });
    }

    // Sort
    if (query.orderBy) {
      if (jsData.utils.isString(query.orderBy)) {
        query.orderBy = [[query.orderBy, 'asc']];
      }
      for (var i = 0; i < query.orderBy.length; i++) {
        if (jsData.utils.isString(query.orderBy[i])) {
          query.orderBy[i] = [query.orderBy[i], 'asc'];
        }
        sqlBuilder = sqlBuilder.orderBy(query.orderBy[i][0], (query.orderBy[i][1] || '').toUpperCase() === 'DESC' ? 'desc' : 'asc');
      }
    }

    // Offset
    if (query.skip) {
      sqlBuilder = sqlBuilder.offset(+query.skip);
    }

    // Limit
    if (query.limit) {
      sqlBuilder = sqlBuilder.limit(+query.limit);
    }

    return sqlBuilder;
    // if (!isEmpty(params.where)) {
    //   forOwn(params.where, (criteria, field) => {
    //     if (contains(field, '.')) {
    //       if (contains(field, ',')) {
    //         let splitFields = field.split(',').map(c => c.trim())
    //         field = splitFields.map(splitField => processRelationField.call(this, resourceConfig, query, splitField, criteria, options, joinedTables)).join(',')
    //       } else {
    //         field = processRelationField.call(this, resourceConfig, query, field, criteria, options, joinedTables)
    //       }
    //     }
    //   })
    // }
  },


  /**
   * Resolve the predicate function for the specified operator based on the
   * given options and this adapter's settings.
   *
   * @name SqlAdapter#getOperator
   * @method
   * @param {string} operator The name of the operator.
   * @param {Object} [opts] Configuration options.
   * @param {Object} [opts.operators] Override the default predicate functions
   * for specified operators.
   * @return {*} The predicate function for the specified operator.
   */
  getOperator: function getOperator(operator, opts) {
    opts || (opts = {});
    opts.operators || (opts.operators = {});
    var ownOps = this.operators || {};
    return jsData.utils.isUndefined(opts.operators[operator]) ? ownOps[operator] : opts.operators[operator];
  },
  getTable: function getTable(mapper) {
    return mapper.table || underscore(mapper.name);
  },
  selectTable: function selectTable(mapper, opts) {
    var self = this;
    opts || (opts = {});
    var query = jsData.utils.isUndefined(opts.query) ? self.knex : opts.query;
    var table = self.getTable(mapper);

    return query.select(table + '.*').from(table);
  }
});

/**
 * Details of the current version of the `js-data-sql` module.
 *
 * @name module:js-data-sql.version
 * @type {Object}
 * @property {string} version.full The full semver value.
 * @property {number} version.major The major version number.
 * @property {number} version.minor The minor version number.
 * @property {number} version.patch The patch version number.
 * @property {(string|boolean)} version.alpha The alpha version value,
 * otherwise `false` if the current version is not alpha.
 * @property {(string|boolean)} version.beta The beta version value,
 * otherwise `false` if the current version is not beta.
 */
var version = {
  beta: 1,
  full: '1.0.0-beta.1',
  major: 1,
  minor: 0,
  patch: 0
};

exports.OPERATORS = OPERATORS;
exports.SqlAdapter = SqlAdapter;
exports.version = version;
exports['default'] = SqlAdapter;
//# sourceMappingURL=js-data-sql.js.map