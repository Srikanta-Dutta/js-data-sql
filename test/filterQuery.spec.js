describe('DSSqlAdapter#filterQuery', function () {
  var adapter
  beforeEach(function () {
    adapter = this.$$adapter
  })
  it('should use custom query', function () {
    var query = adapter.knex.from('test')
    var filterQuery = adapter.filterQuery(query)
    var expectedQuery = adapter.knex
      .from('test')

    assert.equal(filterQuery.toString(), expectedQuery.toString())
  })
  it('should apply where from params to query', function () {
    var query = adapter.knex.from('test')
    var filterQuery = adapter.filterQuery(query, { name: 'Sean' })
    var expectedQuery = adapter.knex
      .from('test')
      .where({name: 'Sean'})

    assert.equal(filterQuery.toString(), expectedQuery.toString())
  })
  it('should apply limit from params to custom query', function () {
    var query = adapter.knex.from('test')
    var filterQuery = adapter.filterQuery(query, { limit: 2 })
    var expectedQuery = adapter.knex
      .from('test')
      .limit(2)

    assert.equal(filterQuery.toString(), expectedQuery.toString())
  })
  it('should apply order from params to custom query', function () {
    var query = adapter.knex.from('test')
    var filterQuery = adapter.filterQuery(query, { orderBy: 'name' })
    var expectedQuery = adapter.knex
      .from('test')
      .orderBy('name', 'asc')

    assert.equal(filterQuery.toString(), expectedQuery.toString())
  })
  it('should convert == null to IS NULL', function () {
    var query = adapter.knex.from('test')
    var filterQuery = adapter.filterQuery(query, { name: { '==' : null } })
    var expectedQuery = adapter.knex
      .from('test')
      .whereNull('name')

    assert.equal(filterQuery.toString(), expectedQuery.toString())
  })
  it('should convert != null to IS NOT NULL', function () {
    var query = adapter.knex.from('test')
    var filterQuery = adapter.filterQuery(query, { name: { '!=' : null } })
    var expectedQuery = adapter.knex
      .from('test')
      .whereNotNull('name')

    assert.equal(filterQuery.toString(), expectedQuery.toString())
  })
  it('should convert |== null to OR field IS NULL', function () {
    var query = adapter.knex.from('test')
    var filterQuery = adapter.filterQuery(query, { name: 'Sean', age: { '|==' : null } })
    var expectedQuery = adapter.knex
      .from('test')
      .where('name', 'Sean')
      .orWhereNull('age')

    assert.equal(filterQuery.toString(), expectedQuery.toString())
  })
  it('should convert |!= null to OR field IS NOT NULL', function () {
    var query = adapter.knex.from('test')
    var filterQuery = adapter.filterQuery(query, { name: 'Sean', age: { '|!=' : null } })
    var expectedQuery = adapter.knex
      .from('test')
      .where('name', 'Sean')
      .orWhereNotNull('age')

    assert.equal(filterQuery.toString(), expectedQuery.toString())
  })
  describe('Custom/override query operators', function () {
    it('should use custom query operator if provided', function () {
      var query = adapter.knex
        .from('user')
        .select('user.*')
      adapter.operators.equals = (sql, field, value) => sql.where(field, value)
      var filterQuery = adapter.filterQuery(query, { name: { equals: 'Sean' } })
      var expectedQuery = adapter.knex
        .from('user')
        .select('user.*')
        .where('name', 'Sean')

      assert.equal(filterQuery.toString(), expectedQuery.toString())
    })
    it('should override built-in operator with custom query operator', function () {
      var query = adapter.knex
        .from('user')
        .select('user.*')
      adapter.operators['=='] = (query, field, value) => query.where(field, '!=', value)
      var filterQuery = adapter.filterQuery(query, { name: { "==": "Sean" }})
      var expectedQuery = adapter.knex
        .from('user')
        .select('user.*')
        .where('name', '!=', 'Sean')

      assert.equal(filterQuery.toString(), expectedQuery.toString())
    })
  })
})
