"use strict";

const task = require('xcane').task;
const metaDb =  require('../index');
const expect = require('chai').expect;
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

  describe('#put()', () => {
    it('should correctly put value into storage', done => {
      task.spawn(function* () {
        yield metaDb.put('sample-key', 'a value');
        expect(yield metaDb.get('sample-key')).to.be.equal('a value');
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

  describe('#transactionManagement', () => {
    it('should manage transaction correctly', done => {
      sequelize.transaction(t => task.spawn(function* () {
        yield metaDb.put('my-tran-key', 'my-value');
        expect(yield metaDb.get('my-tran-key')).to.be.equal('my-value');
        throw new Error('oops');
      })).then(() => done('it should not had succeeded')).catch(err => {
        expect(err).to.be.an.instanceof(Error);
        expect(err.message).to.be.equal('oops');

        task.spawn(function* () {
          expect(yield metaDb.getOrDefault('my-tran-key', 'default'))
            .to.be.equal('default');
        }).then(() => done()).catch(done);
      }).catch(done);
    });
  });
});
