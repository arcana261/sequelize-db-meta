"use strict";

const task = require('xcane').task;
const iterable = require('xcane').iterable;
const metaDb =  require('../index');
const expect = require('chai').expect;
const promise = require('xcane').promise;
const Sequelize = require('sequelize');

describe('sequelize-db-meta', () => {
  let sequelize = null;

  before(done => {
    sequelize = new Sequelize({
      dialect: 'sqlite',
      storage: ':memory:',
      benchmark: true
    });

    metaDb.init(sequelize);

    sequelize.sync().then(() => done()).catch(done);
  });

  beforeEach(done => {
    metaDb.clear().then(() => done()).catch(done);
  });

  describe('#put()', () => {
    it('should correctly put value into storage', done => {
      task.spawn(function* () {
        yield metaDb.put('sample-key', 'a value');
        expect(yield metaDb.get('sample-key')).to.be.equal('a value');
      }).then(() => done()).catch(done);
    });

    it('should over-write previous value', done => {
      task.spawn(function* () {
        yield metaDb.put('prev-key', 'value');
        yield metaDb.put('prev-key', 'new-value');
        expect(yield metaDb.get('prev-key')).to.be.equal('new-value');
      }).then(() => done()).catch(done);
    });
  });

  describe('#getOrDefault()', () => {
    it('should return default value if key does not exist', done => {
      task.spawn(function* () {
        expect(yield metaDb.getOrDefault('non-key', {a: 'default'}))
          .to.be.deep.equal({a: 'default'});

        expect(yield metaDb.getOrDefault('non-key', {a: 'default2'}))
          .to.be.deep.equal({a: 'default2'});
      }).then(() => done()).catch(done);
    });

    it('shoudl return value stored at db if key exists', done => {
      task.spawn(function* () {
        yield metaDb.put('my:key', 123);
        expect(yield metaDb.getOrDefault('my:key', 'def'))
          .to.be.equal(123);
      }).then(() => done()).catch(done);
    });
  });

  describe('#getOrNull()', () => {
    it('should return null value if key does not exist', done => {
      task.spawn(function* () {
        expect(yield metaDb.getOrNull('non-key'))
          .to.be.null;
      }).then(() => done()).catch(done);
    });

    it('should return value stored at db if key exists', done => {
      task.spawn(function* () {
        yield metaDb.put('qwe:eee', 'just some value');
        expect(yield metaDb.getOrNull('qwe:eee'))
          .to.be.equal('just some value');
      }).then(() => done()).catch(done);
    });
  });

  describe('#get()', () => {
    it('should throw error if key does not exist', done => {
      metaDb.get('non-key')
        .then(() => done('it should not had passed'))
        .catch(err => {
          expect(err).to.be.an.instanceof(Error);
          expect(err.message).to.be.equal('key not found: non-key');
          done();
        }).catch(done);
    });

    it('should return correct value if key exists', done => {
      task.spawn(function* () {
        yield metaDb.put('the king', 'is here');
        expect(yield metaDb.get('the king')).to.be.equal('is here');
      }).then(() => done()).catch(done);
    });
  });

  describe('#count()', () => {
    it('should count items correctly', done => {
      task.spawn(function* () {
        expect(yield metaDb.count()).to.be.equal(0);
        yield metaDb.put('key-1', 'value-1');
        expect(yield metaDb.count()).to.be.equal(1);
        yield metaDb.put('key-2', 'value-2');
        expect(yield metaDb.count()).to.be.equal(2);
        yield metaDb.put('qqq-1', 'www-1');
        expect(yield metaDb.count()).to.be.equal(3);
        expect(yield metaDb.count('key-*')).to.be.equal(2);
        expect(yield metaDb.count('qqq-*')).to.be.equal(1);
        expect(yield metaDb.count('~~~~')).to.be.equal(0);
      }).then(() => done()).catch(done);
    });
  });

  describe('#clear()', () => {
    it('should clear all items', done => {
      task.spawn(function* () {
        expect(yield metaDb.count()).to.be.equal(0);
        expect(yield metaDb.getOrNull('key')).to.be.null;
        yield metaDb.put('key', 'value');
        expect(yield metaDb.count()).to.be.equal(1);
        expect(yield metaDb.get('key')).to.be.equal('value');
        yield metaDb.clear();
        expect(yield metaDb.count()).to.be.equal(0);
        expect(yield metaDb.getOrNull('key')).to.be.null;
      }).then(() => done()).catch(done);
    });
  });

  describe('#all()', () => {
    it('should list items correctly', done => {
      task.spawn(function* () {
        yield metaDb.put('key-1', 'value-1');
        yield metaDb.put('key-2', 'value-2');
        yield metaDb.put('qqq-1', 'www-1');
        expect(iterable.from(yield metaDb.all())
          .select(x => [x.key, x.value])
          .orderBy()
          .toArray()).to.be.deep.equal(
            [['key-1', 'value-1'], ['key-2', 'value-2'], ['qqq-1', 'www-1']]);
        expect(iterable.from([]).concat(
          yield metaDb.all(0, 1),
          yield metaDb.all(1, 2)
        ).select(x => [x.key, x.value]).orderBy().toArray())
        .to.be.deep.equal([
          ['key-1', 'value-1'], ['key-2', 'value-2'], ['qqq-1', 'www-1']
        ]);
        expect(iterable.from(yield metaDb.all(null, null, 'key-*'))
          .select(x => [x.key, x.value])
          .orderBy()
          .toArray()).to.be.deep.equal(
            [['key-1', 'value-1'], ['key-2', 'value-2']]);
      }).then(() => done()).catch(done);
    });
  });

  describe('#expire()', () => {
    it('should fail with non-existing key', done => {
      metaDb.expire('non-key', 1000)
        .then(() => done('it should not had succeeded'))
        .catch(err => {
          expect(err).to.be.an.instanceof(Error);
          expect(err.message).to.be.equal(`key not found: non-key`);
          done();
        }).catch(done);
    });

    it('should correctly set expire on objects', function(done) {
      this.timeout(5000);

      task.spawn(function* () {
        yield metaDb.put('key-1', 'value-1');
        yield metaDb.put('key-2', 'value-2');
        expect(yield metaDb.get('key-1')).to.be.equal('value-1');
        expect(yield metaDb.get('key-2')).to.be.equal('value-2');
        yield metaDb.expire('key-1', 1);
        expect(yield metaDb.get('key-1')).to.be.equal('value-1');
        expect(yield metaDb.get('key-2')).to.be.equal('value-2');
        yield promise.delay(500);
        expect(yield metaDb.get('key-1')).to.be.equal('value-1');
        expect(yield metaDb.get('key-2')).to.be.equal('value-2');
        yield promise.delay(600);
        expect(yield metaDb.getOrNull('key-1')).to.be.null;
        expect(yield metaDb.get('key-2')).to.be.equal('value-2');
      }).then(() => done()).catch(done);
    });
  });

  describe('#gc()', () => {
    it('should remove expired items correctly', function (done) {
      this.timeout(5000);

      task.spawn(function* () {
        yield metaDb.put('key-1', 'value-1');
        yield metaDb.put('key-2', 'value-2');
        expect(yield metaDb.get('key-1')).to.be.equal('value-1');
        expect(yield metaDb.get('key-2')).to.be.equal('value-2');
        yield metaDb.expire('key-1', 1);
        yield promise.delay(1100);
        expect(yield metaDb.getOrNull('key-1')).to.be.null;
        expect(yield metaDb.get('key-2')).to.be.equal('value-2');
        expect(yield metaDb.count(null, {expires: {$ne: null}})).to.be.equal(1);
        yield metaDb.gc();
        expect(yield metaDb.get('key-2')).to.be.equal('value-2');
        expect(yield metaDb.count(null, {expires: {$ne: null}})).to.be.equal(0);
      }).then(() => done()).catch(done);
    });
  });

  describe('#monitor()', () => {
    it('should auto-schedule correctly', function(done) {
      this.timeout(10000);

      task.spawn(function* () {
        let db = new metaDb.MetaDB(sequelize, metaDb.tableName());
        yield db.put('key-1', 'value-1');
        yield db.put('key-2', 'value-2');
        expect(yield db.get('key-1')).to.be.equal('value-1');
        expect(yield metaDb.has('key-1')).to.be.true;
        yield db.expire('key-1', 1);
        yield promise.delay(1100);
        expect(yield metaDb.getOrNull('key-1')).to.be.null;
        expect(yield metaDb.has('key-1')).to.be.false;
        expect(yield metaDb.count(null, {expires: {$ne: null}})).to.be.equal(1);
        db.monitor('*/1 * * * * *');
        yield promise.delay(1100);
        expect(yield metaDb.count(null, {expires: {$ne: null}})).to.be.equal(0);
        expect(yield metaDb.has('key-1')).to.be.false;
        expect(yield metaDb.delete('key-1')).to.be.false;
        expect(yield metaDb.has('key-2')).to.be.true;
        expect(yield metaDb.get('key-2')).to.be.equal('value-2');
        expect(metaDb.__getDestroyCounter()).to.be.equal(0);
        db = null;
        global.gc();
        yield promise.delay(1100);
        expect(metaDb.__getDestroyCounter()).to.be.equal(1);
      }).then(() => done()).catch(done);
    });
  });

  describe('#delete()', () => {
    it('should delete a key accordingly', done => {
      task.spawn(function* () {
        yield metaDb.put('key-1', 'value-1');
        expect(yield metaDb.has('key-1')).to.be.true;
        expect(yield metaDb.delete('key-1')).to.be.true;
        expect(yield metaDb.has('key-1')).to.be.false;
        expect(yield metaDb.delete('key-1')).to.be.false;
      }).then(() => done()).catch(done);
    });
  });

  describe('#has()', () => {
    it('should correctly show existence of items', done => {
      task.spawn(function* () {
        yield metaDb.put('key-1', 'value-1');
        expect(yield metaDb.get('key-1')).to.be.equal('value-1');
        expect(yield metaDb.has('key-1')).to.be.true;
        expect(yield metaDb.has('key-2')).to.be.false;
      }).then(() => done()).catch(done);
    });
  });

  describe('#prefix()', () => {
    it('should prefix keys', done => {
      task.spawn(function* () {
        yield metaDb.put('key-2', 'value-2');
        const p = metaDb.prefix('pre-');
        yield p.put('key-1', 'value-1');
        expect(yield p.get('key-1')).to.be.equal('value-1');
        expect(yield metaDb.has('key-1')).to.be.false;
        expect(yield metaDb.getOrNull('key-1')).to.be.null;
        expect(yield metaDb.has('pre-key-1')).to.be.true;
        expect(yield metaDb.get('pre-key-1')).to.be.equal('value-1');
        expect(yield p.has('key-1')).to.be.true;
        expect(yield p.getOrNull('key-1')).to.be.equal('value-1');
        expect(yield p.getOrDefault('key-1', 'default')).to.be.equal('value-1');
        expect(yield p.count()).to.be.equal(1);
        expect(yield metaDb.count()).to.be.equal(2);
        expect(iterable.from(yield p.all()).select(x => x.value).toArray()).to.be.deep.equal(['value-1']);
        expect(iterable.from(yield p.all()).select(x => x.key).toArray()).to.be.deep.equal(['key-1']);
        expect(yield p.count('key-*')).to.be.equal(1);
        expect(iterable.from(yield p.all(null, null, 'key-*')).select(x => x.value).toArray()).to.be.deep.equal(['value-1']);
        yield p.put('qqq', 'www');
        expect(yield p.count()).to.be.equal(2);
        expect(yield p.count('key-*')).to.be.equal(1);
        expect(iterable.from(yield p.all(null, null, 'key-*')).select(x => x.value).toArray()).to.be.deep.equal(['value-1']);
        expect(iterable.from(yield p.all()).select(x => x.value).orderBy().toArray()).to.be.deep.equal(['value-1', 'www']);
      }).then(() => done()).catch(done);
    });

    it('should prefix itself', done => {
      task.spawn(function* () {
        const p = metaDb.prefix('pre-');
        const q = p.prefix('post-');
        yield q.put('key-1', 'value-1');
        expect(yield q.get('key-1')).to.be.equal('value-1');
        expect(yield p.get('post-key-1')).to.be.equal('value-1');
        expect(yield metaDb.get('pre-post-key-1')).to.be.equal('value-1');
        expect((yield q.all()).map(x => [x.key, x.value])).to.be.deep.equal([['key-1', 'value-1']]);
        expect((yield p.all()).map(x => [x.key, x.value])).to.be.deep.equal([['post-key-1', 'value-1']]);
        expect((yield metaDb.all()).map(x => [x.key, x.value])).to.be.deep.equal([['pre-post-key-1', 'value-1']]);
      }).then(() => done()).catch(done);
    });
  });

  describe('#transactionManagement', () => {
    it('should manage transaction correctly', done => {
      sequelize.transaction(t => task.spawn(function* () {
        yield metaDb.put('my-tran-key', 'my-value');
        expect(yield metaDb.get('my-tran-key')).to.be.equal('my-value');
        throw new Error('oops');
        expect(yield metaDb.count()).to.be.equal(1);
      })).then(() => done('it should not had succeeded')).catch(err => {
        expect(err).to.be.an.instanceof(Error);
        expect(err.message).to.be.equal('oops');

        task.spawn(function* () {
          expect(yield metaDb.getOrDefault('my-tran-key', 'default'))
            .to.be.equal('default');
          expect(yield metaDb.count()).to.be.equal(0);
        }).then(() => done()).catch(done);
      }).catch(done);
    });
  });

  describe('#assign()', () => {
    it('should add new scalar value to empty db', () =>
      task.spawn(function* () {
        yield metaDb.assign('my-key', 10);
        expect(yield metaDb.get('my-key')).to.be.equal(10);
        yield metaDb.clear();
        yield metaDb.assign('my-key', 'hello');
        expect(yield metaDb.get('my-key')).to.be.equal('hello');
        yield metaDb.clear();
        yield metaDb.assign('my-key', true);
        expect(yield metaDb.get('my-key')).to.be.equal(true);
        yield metaDb.clear();
        yield metaDb.assign('my-key', false);
        expect(yield metaDb.get('my-key')).to.be.equal(false);
        yield metaDb.clear();
        yield metaDb.assign('my-key', null);
        expect(yield metaDb.get('my-key')).to.be.null;
        yield metaDb.clear();
        yield metaDb.assign('my-key', undefined);
        expect(yield metaDb.get('my-key')).to.be.undefined;
      }));

    it('should add new scalar over another scalar', () =>
      task.spawn(function* () {
        yield metaDb.assign('my-key', 10);
        expect(yield metaDb.get('my-key')).to.be.equal(10);
        yield metaDb.assign('my-key', 'hello');
        expect(yield metaDb.get('my-key')).to.be.equal('hello');
        yield metaDb.assign('my-key', true);
        expect(yield metaDb.get('my-key')).to.be.equal(true);
        yield metaDb.assign('my-key', false);
        expect(yield metaDb.get('my-key')).to.be.equal(false);
        yield metaDb.assign('my-key', null);
        expect(yield metaDb.get('my-key')).to.be.null;
        yield metaDb.assign('my-key', undefined);
        expect(yield metaDb.get('my-key')).to.be.undefined;
      }));

    it('should assign complex object over scalar', () =>
      task.spawn(function* () {
        yield metaDb.assign('my-key', 10);
        expect(yield metaDb.get('my-key')).to.be.equal(10);
        yield metaDb.assign('my-key', {yyy: 'hello'});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: 'hello'
        });
        yield metaDb.clear();
        yield metaDb.assign('my-key', 'hello');
        expect(yield metaDb.get('my-key')).to.be.equal('hello');
        yield metaDb.assign('my-key', {yyy: 'hello'});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: 'hello'
        });
        yield metaDb.clear();
        yield metaDb.assign('my-key', true);
        expect(yield metaDb.get('my-key')).to.be.equal(true);
        yield metaDb.assign('my-key', {yyy: 'hello'});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: 'hello'
        });
        yield metaDb.clear();
        yield metaDb.assign('my-key', false);
        expect(yield metaDb.get('my-key')).to.be.equal(false);
        yield metaDb.assign('my-key', {yyy: 'hello'});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: 'hello'
        });
        yield metaDb.clear();
        yield metaDb.assign('my-key', null);
        expect(yield metaDb.get('my-key')).to.be.null;
        yield metaDb.assign('my-key', {yyy: 'hello'});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: 'hello'
        });
        yield metaDb.clear();
        yield metaDb.assign('my-key', undefined);
        expect(yield metaDb.get('my-key')).to.be.undefined;
        yield metaDb.assign('my-key', {yyy: 'hello'});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: 'hello'
        });
      }));

    it('should assign scalar over complex object', () =>
      task.spawn(function* () {
        yield metaDb.assign('my-key', {yyy: 'hello'});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: 'hello'
        });
        yield metaDb.assign('my-key', 10);
        expect(yield metaDb.get('my-key')).to.be.equal(10);
        yield metaDb.clear();
        yield metaDb.assign('my-key', {yyy: 'hello'});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: 'hello'
        });
        yield metaDb.assign('my-key', 'hello');
        expect(yield metaDb.get('my-key')).to.be.equal('hello');
        yield metaDb.clear();
        yield metaDb.assign('my-key', {yyy: 'hello'});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: 'hello'
        });
        yield metaDb.assign('my-key', true);
        expect(yield metaDb.get('my-key')).to.be.equal(true);
        yield metaDb.clear();
        yield metaDb.assign('my-key', {yyy: 'hello'});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: 'hello'
        });
        yield metaDb.assign('my-key', false);
        expect(yield metaDb.get('my-key')).to.be.equal(false);
        yield metaDb.clear();
        yield metaDb.assign('my-key', {yyy: 'hello'});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: 'hello'
        });
        yield metaDb.assign('my-key', null);
        expect(yield metaDb.get('my-key')).to.be.null;
        yield metaDb.clear();
        yield metaDb.assign('my-key', {yyy: 'hello'});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: 'hello'
        });
        yield metaDb.assign('my-key', undefined);
        expect(yield metaDb.get('my-key')).to.be.undefined;
      }));

    it('should assign complex object over complex objects', () =>
      task.spawn(function* () {
        yield metaDb.assign('my-key', {yyy: 'hello'});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: 'hello'
        });
        yield metaDb.assign('my-key', {xxx: 52});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: 'hello',
          xxx: 52
        });
        yield metaDb.assign('my-key', {yyy: true});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: true,
          xxx: 52
        });
        yield metaDb.assign('my-key', {yyy: {a: 5}});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: {a: 5},
          xxx: 52
        });
        yield metaDb.assign('my-key', {yyy: {b: 10}});
        expect(yield metaDb.get('my-key')).to.be.deep.equal({
          yyy: {b: 10},
          xxx: 52
        });
      }));
  });
});
