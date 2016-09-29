"use strict";

const metaDb =  require('../index');
const expect = require('chai').expect;
const Sequelize = require('sequelize');

describe('sequelize-db-meta', () => {
  let sequelize = null;

  before(done => {
    sequelize = new Sequelize({
      dialect: 'sqlite',
      storage: ':memory:'
    });

    metaDb.init(sequelize);

    sequelize.sync().then(() => done()).catch(done);
  });

  describe('#_get()', () => {
    it('should not find if key does not exist', done => {
      metaDb._get('sample_key').then(() => {
        done();
      }).catch(done);
    });
  });
});
