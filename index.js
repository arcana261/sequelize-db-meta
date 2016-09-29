"use strict";

const task = require('xcane').task;
const type = require('xcane').type;
const Sequelize = require('sequelize');
let Meta = null;

/**
 * @desc Provides a usable mechanism to store some metadata in form of
 * key-value pairs in a relational database
 * @author Mohamad mehdi Kharatizadeh - m_kharatizadeh@yahoo.com
 */
class SequelizeDbMeta {
  /**
   * @desc initialize model definitions in database
   * @param {Sequelize} sequelize - a reference to an instance of Sequelize
   */
  static init(sequelize) {
    Meta = sequelize.define('meta', {
      key: {
        type: Sequelize.TEXT,
        primaryKey: true
      },
      value: {
        type: Sequelize.TEXT,
        allowNull: false
      }
    }, {
      timestamps: false
    });
  }

  /**
   * @desc internal function to query a single key from database
   * @param {string} key - the key to requested value
   * @param {*=} transaction - optional reference to transaction object
   * @return {Promise.<*>} - value stored at key
   */
  static _get(key, transaction) {
    return task.spawn(function* () {
      let res = yield Meta.findOne(Object.assign({
        where: {
          key: key
        },
        attributes: ['value']
      }, type.isOptional(transaction) ? null : {transaction: transaction}));

      if (res === null) {
        return {
          value: null,
          found: false
        };
      }

      return {
        value: JSON.parse(res.value).value,
        found: true
      };
    });
  }

  /**
   * @desc get value stored at key or a default value
   * @param {string} key - key to requested vaue
   * @param {*} value - default value to use if key is not found
   * @param {*=} transaction - optional sequelize transaction objet
   * @return {Promise.<*>} - returns value stored at key or a default value
   */
  static getOrDefault(key, value, transaction) {
    return SequelizeDbMeta._get(key, transaction).then(v => {
      if (!v.found) {
        return Promise.resolve(value);
      }

      return Promise.resolve(v.value);
    });
  }

  /**
   * @desc gets value stored at key or null value
   * @param {string} key - key to requested value
   * @param {*=} transaction - optional transaction object
   * @return {Promise.<*|null>} - value stored at requested key
   */
  static getOrNull(key, transaction) {
    return SequelizeDbMeta.getOrDefault(key, null, transaction);
  }

  /**
   * @desc gets value stored at key or rejects with an error
   * @param {string} key - key to requested value
   * @param {*=} transaction - optional transaction object
   * @return {Promise.<*>} - value stored at requested key
   */
  static get(key, transaction) {
    return SequelizeDbMeta._get(key, transaction).then(v => {
      if (!v.found) {
        return Promise.reject(new Error(`key not found: ${key}`));
      }

      return Promise.resolve(v.value);
    });
  }

  /**
   * @desc sets value at target key
   * @param {string} key - key to requested value
   * @param {*} value - any javascript object to store
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise} - resolve when value is created
   */
  static put(key, value, transaction) {
    return Meta.create(Object.assign({
      key: key,
      value: JSON.stringify({value: value})
    }, type.isOptional(transaction) ? null : {transaction: transaction}));
  }
}

module.exports = SequelizeDbMeta;
