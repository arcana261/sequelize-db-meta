"use strict";

const task = require('xcane').task;
const type = require('xcane').type;
const cron = require('node-cron');
const weak = require('weak');
let __destroyCounter = 0;

/**
 * @desc issue a garbage collector monitor without risk of memory leak
 * @param {Weakref} ref - weak reference to object
 * @param {string} schedule - cron compatible schedule
 * @return {*} - a task object
 */
function scheduleGc(ref, schedule) {
  let task = cron.schedule(schedule, () => {
    const instance = weak.get(ref);

    if (type.isOptional(instance)) {
      task.destroy();
      task = null;
      __destroyCounter++;
    } else {
      instance.gc();
    }
  });

  task.start();
  return task;
}

/**
 * @desc allows for settings prefix to keys
 * @author Mohamad mehdi Kharatizadeh - m_kharatizadeh@yahoo.com
 * @private
 */
class _SequelizeDbMetaPrefixInstance {
  /**
   * @desc creates a new instance of _SequelizeDbMetaPrefixInstance
   * @param {SequelizeDbMetaInstance} master - instance to master
   * @param {string} prefix - prefix to use
   */
  constructor(master, prefix) {
    this._master = master;
    this._prefix = prefix;
  }

  /**
   * @desc gets table name
   * @return {string} - table name
   */
  get tableName() {
    return this._master.tableName();
  }

  /**
   * @desc converts key and applies prefix
   * @param {string} key - key to object
   * @return {string} - converted key
   * @private
   */
  _convertKey(key) {
    if (type.isOptional(key)) {
      return `${this._prefix}*`;
    }

    return `${this._prefix}${key}`;
  }

  /**
   * @desc gets value associated with a key or default if it does not exist
   * @param {string} key - object key
   * @param {*} value - default value to use if object does not exist
   * @param {*=} transaction - optional sequelize transaction
   * @return {Promise.<*>} - returns value at key or a default value
   */
  getOrDefault(key, value, transaction) {
    return this._master.getOrDefault(
      this._convertKey(key), value, transaction);
  }

  /**
   * @desc gets value associated with a key or null if it does not exist
   * @param {string} key - object key
   * @param {*=} transaction - optional sequelize transaction to use
   * @return {Promise.<*|null>} - value at key or a null
   */
  getOrNull(key, transaction) {
    return this._master.getOrNull(this._convertKey(key), transaction);
  }

  /**
   * @desc gets value stored at key or rejects with an error
   * @param {string} key - object key
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise.<*>} - value stored at key
   */
  get(key, transaction) {
    return this._master.get(this._convertKey(key), transaction);
  }

  /**
   * @desc shows whether key is inside storage or not
   * @param {string} key - object key
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise.<boolean>} - true if key exists
   */
  has(key, transaction) {
    return this._master.has(this._convertKey(key), transaction);
  }

  /**
   * @desc deletes value from storage, returns true if value was deleted
   * @param {string} key - object key
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise.<boolean>} - true if value was deleted
   */
  delete(key, transaction) {
    return this._master.delete(this._convertKey(key), transaction);
  }

  /**
   * @desc adds value to database or overwrites existing value
   * @param {string} key - object key
   * @param {*} value - new object value
   * @param {*=} data - additional data to write
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise} - resolves when value was added
   */
  put(key, value, data, transaction) {
    return this._master.put(this._convertKey(key), value, data, transaction);
  }

  /**
   * @desc merge previous value of key into new value
   * @param {string} key - key to requested value
   * @param {*} value - any javascript object to store
   * @param {*=} data - additional optional data to write
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise} - resolve when value is created
   */
  assign(key, value, data, transaction) {
    return this._master.assign(
      this._convertKey(key), value, data, transaction);
  }

  /**
   * @desc sets expiration time on key in seconds
   * @param {string} key - object key
   * @param {number} time - time in seconds
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise} - resolves when value was added
   */
  expire(key, time, transaction) {
    return this._master.expire(key, time, transaction);
  }

  /**
   * @desc collects garbage and removes expired items
   * @return {Promise} - resolves when garbage collection is done
   */
  gc() {
    return this._master.gc();
  }

  /**
   * @desc start automatic garbage collection service based on cron-tab
   * scheduling specification
   * @param {string} schedule - CRON-tab schedule
   */
  monitor(schedule) {
    this._master.monitor(schedule);
  }

  /**
   * @desc clear everything on database
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise} - resolve when database is truncated
   */
  clear(transaction) {
    return this._master.clear(transaction);
  }

  /**
   * @desc count number of records using a pattern
   * @param {string=} pattern - optional pattern to keys
   * @param {*=} where - optional additional where clause
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise.<number>} - resolves to number of records
   */
  count(pattern, where, transaction) {
    return this._master.count(this._convertKey(pattern), where, transaction);
  }

  /**
   * @desc lists records found in database using a pattern
   * @param {number=} start - optional start of lookup
   * @param {length=} length - optional length of data
   * @param {string=} pattern - optional wildcard pattern of keys
   * @param {*=} where - optional additions to where clause
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise.<Array.<*> >} - filtered records
   */
  all(start, length, pattern, where, transaction) {
    return this._master.all(
      start, length, this._convertKey(pattern), where, transaction)
      .then(arr => Promise.resolve(arr.map(record => {
        const key = record.key.substr(this._prefix.length);
        const prototype = Object.create(Object.getPrototypeOf(record));

        Object.defineProperty(prototype, 'key', {
          get: () => key
        });
        Object.setPrototypeOf(record, prototype);

        return record;
      })));
  }

  /**
   * @desc add additional prefix to key lookup
   * @param {string} newPrefix - new prefix to use
   * @return {SequelizeDbMetaInstance} - new storage with additional prefix
   */
  prefix(newPrefix) {
    return new _SequelizeDbMetaPrefixInstance(
      this._master, this._convertKey(newPrefix));
  }
}

/**
 * @desc Provides a usable extensible mechanism to store some metadata
 * in form of key-value pairs in a relational database
 * @author Mohamad mehdi Kharatizadeh - m_kharatizadeh@yahoo.com
 */
class SequelizeDbMetaInstance {
  /**
   * @desc initialize model definitions in database
   * @param {Sequelize} sequelize - a reference to an instance of Sequelize
   * @param {string=} name - name of meta table
   * @param {*=} definitions - extra definitions to use
   * @param {*=} options - optional optional to pass
   */
  constructor(sequelize, name, definitions, options) {
    if (type.isOptional(name)) {
      name = '__metadb';
    }

    options = Object.assign({
      timestamps: false
    }, options);

    options.index = (options.index || []).concat([{
      name: 'expires_index',
      method: 'btree',
      fields: [{
        attribute: 'expires',
        order: 'desc'
      }]
    }]);

    this._table = sequelize.define(name, Object.assign({
      key: {
        type: sequelize.Sequelize.TEXT,
        primaryKey: true
      },
      value: {
        type: sequelize.Sequelize.TEXT,
        allowNull: false,
        get: function parseValue() {
          const value = this.getDataValue('value');

          if (type.isUndefined(value)) {
            return undefined;
          }

          return JSON.parse(value).value;
        },
        set: function encodeValue(value) {
          this.setDataValue('value', JSON.stringify({value: value}));
        }
      },
      expires: {
        type: sequelize.Sequelize.DATE,
        allowNull: true
      }
    }, definitions), options);

    this._sequelize = sequelize;
    this._task = null;
    this._tableName = name;
  }

  /**
   * @return {string} - table name used for operations
   */
  get tableName() {
    return this._tableName;
  }

  /**
   * @desc internal function to query a single key from database
   * @param {string} key - the key to requested value
   * @param {*=} transaction - optional reference to transaction object
   * @return {Promise.<*>} - value stored at key
   * @private
   */
  _get(key, transaction) {
    const self = this;

    return task.spawn(function * task() {
      let res = yield self._table.findOne(Object.assign({
        where: {
          key: key,
          expires: {
            $or: {
              $eq: null,
              $gt: new Date()
            }
          }
        },
        attributes: {
          exclude: ['key']
        }
      }, type.isOptional(transaction) ? null : {transaction: transaction}));

      if (res === null) {
        return {
          value: null,
          found: false
        };
      }

      return {
        value: res.value,
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
  getOrDefault(key, value, transaction) {
    return this._get(key, transaction).then(v => {
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
  getOrNull(key, transaction) {
    return this.getOrDefault(key, null, transaction);
  }

  /**
   * @desc gets value stored at key or rejects with an error
   * @param {string} key - key to requested value
   * @param {*=} transaction - optional transaction object
   * @return {Promise.<*>} - value stored at requested key
   */
  get(key, transaction) {
    return this._get(key, transaction).then(v => {
      if (!v.found) {
        return Promise.reject(new Error(`key not found: ${key}`));
      }

      return Promise.resolve(v.value);
    });
  }

  /**
   * @desc finds whether metadb contains requested key
   * @param {string} key - key to resource
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise.<boolean>} - whether key is present
   */
  has(key, transaction) {
    return this._get(key, transaction).then(v => {
      if (!v.found) {
        return Promise.resolve(false);
      }

      return Promise.resolve(true);
    });
  }

  /**
   * @desc removes item from storage. returns true if item was found
   * @param {string} key - target key
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise.<boolean>} - true if item existed
   */
  delete(key, transaction) {
    return this._table.destroy(Object.assign({
      where: {
        key: key,
        expires: {
          $or: {
            $eq: null,
            $gt: new Date()
          }
        }
      }
    }, type.isOptional(transaction) ? null : {
      transaction: transaction
    })).then(num => Promise.resolve(num > 0));
  }

  /**
   * @desc sets value at target key
   * @param {string} key - key to requested value
   * @param {*} value - any javascript object to store
   * @param {*=} data - additional optional data to write
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise} - resolve when value is created
   */
  put(key, value, data, transaction) {
    return this._table.upsert(Object.assign({
      key: key,
      value: value,
      expires: null
    }, data), type.isOptional(transaction) ? null : {
      transaction: transaction
    });
  }

  /**
   * @desc merge previous value of key into new value
   * @param {string} key - key to requested value
   * @param {*} value - any javascript object to store
   * @param {*=} data - additional optional data to write
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise} - resolve when value is created
   */
  assign(key, value, data, transaction) {
    const self = this;

    return task.spawn(function * task() {
      if (type.isString(value) || type.isNumber(value) ||
        type.isBoolean(value) || type.isOptional(value)) {
        return yield self.put(key, value, data, transaction);
      }

      const current = yield self._get(key, transaction);

      if (!current.found || type.isString(current.value) ||
        type.isNumber(current.value) || type.isBoolean(current.value) ||
        type.isOptional(current.value)) {
        return yield self.put(key, value, data, transaction);
      }

      return yield self.put(key, Object.assign(current.value,
        value), data, transaction);
    });
  }

  /**
   * @desc sets expiration time of key
   * @param {string} key - target key
   * @param {number} time - time in seconds
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise} - resolves when expiration is set
   */
  expire(key, time, transaction) {
    let date = new Date();
    const self = this;
    date.setSeconds(date.getSeconds() + time);

    return task.spawn(function * task() {
      let result = yield self._table.update({
        expires: date
      }, Object.assign({
        where: {
          key: key,
          expires: {
            $or: {
              $eq: null,
              $gt: new Date()
            }
          }
        }
      }, type.isOptional(transaction) ? null : {transaction: transaction}));

      if (result[0] < 1) {
        throw new Error(`key not found: ${key}`);
      }
    });
  }

  /**
   * @desc remove expires items from database
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise} - resolve when expired items are removed
   */
  gc(transaction) {
    return this._table.destroy(Object.assign({
      where: {
        expires: {
          $lte: new Date()
        }
      }
    }, type.isOptional(transaction) ? null : {
      transaction: transaction
    }));
  }

  /**
   * @desc start garbage collection monitoring service
   * @param {string=} schedule - a cron-tab compatible schedule string.
   * default is every 20 minutes
   */
  monitor(schedule) {
    if (!type.isOptional(this._task)) {
      this._task.destroy();
    }

    if (type.isOptional(schedule)) {
      schedule = '*/20 * * * *';
    }

    this._task = scheduleGc(weak(this), schedule);
  }

  /**
   * @desc empty all items in storage
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise} - fulfils when all items are cleared
   */
  clear(transaction) {
    return this._table.truncate(Object.assign({},
       type.isOptional(transaction) ? null : {transaction: transaction}));
  }

  /**
   * @desc get total number of key-value pairs stored in meta data
   * @param {string=} pattern - pattern to search for
   * @param {*=} where - optional additions to where clause
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise.<number>} - total number of items
   */
  count(pattern, where, transaction) {
    const self = this;

    return task.spawn(function * task() {
      let result = yield self._table.findAll(Object.assign({
        attributes: [
          [self._sequelize.fn('COUNT', self._sequelize.col('*')), 'total']
        ],
        where: Object.assign({
          expires: {
            $or: {
              $eq: null,
              $gt: new Date()
            }
          }
        }, type.isOptional(pattern) ? null : {
          key: {
            $like: pattern.replace(/\?/g, '_').replace(/\*/g, '%')
          }
        }, where)
      }, type.isOptional(transaction) ? null : {transaction: transaction}));

      return Number(result[0].get('total'));
    });
  }

  /**
   * @desc returns list of items stored in storage
   * @param {number=} start - offset to start
   * @param {number=} length - number of items
   * @param {string=} pattern - wildcard pattern to match against keys
   * @param {*=} where - optional additions to where clause
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise.<Array.<*> >} - found rows
   */
  all(start, length, pattern, where, transaction) {
    return this._table.findAll(Object.assign({
      where: Object.assign({
        expires: {
          $or: {
            $eq: null,
            $gt: new Date()
          }
        }
      }, type.isOptional(pattern) ? null : {
        key: {
          $like: pattern.replace(/\?/g, '_').replace(/\*/g, '%')
        }
      }, where)
    }, type.isOptional(start) ? null : {
      offset: start
    }, type.isOptional(length) ? null : {
      limit: length
    }, type.isOptional(transaction) ? null : {
      transaction: transaction
    }));
  }

  /**
   * @desc prefixes keys accessed by the instance
   * @param {string} newPrefix - prefix to keys
   * @return {SequelizeDbMetaInstance} - new instance with prefix set
   */
  prefix(newPrefix) {
    return new _SequelizeDbMetaPrefixInstance(this, newPrefix);
  }
}

let _globalInstance = null;
/**
 * @desc Provides extensible key-value pair storage in relational DBMS's
 * @author Mohamad mehdi Kharatizadeh - m_kharatizadeh@yahoo.com
 * @namespace SequelizeDbMeta
 */
module.exports = Object.freeze({
  /**
   * @desc Allows for creating custom meta tables inside DBMS
   * @type SequelizeDbMetaInstance
   */
  MetaDB: SequelizeDbMetaInstance,

  /**
   * @desc initializes global meta instance
   * @param {Sequelize} sequelize - an instance to sequelize
   */
  init: sequelize => {
    _globalInstance = new SequelizeDbMetaInstance(sequelize);
  },

  /**
   * @desc gets value from storage or a default value if key does not exist
   * @param {string} key - target key
   * @param {*} value - target value
   * @param {*=} transaction - an instance to sequelize transaction object
   * @return {Promise.<*>} - value stored at location or a default value
   * @memberof SequelizeDbMeta
   */
  getOrDefault: (key, value, transaction) =>
    _globalInstance.getOrDefault(key, value, transaction),

  /**
   * @desc gets value from stroage or null if value does not exist
   * @param {string} key - target key
   * @param {*=} transaction - an instance to sequelize transaction object
   * @return {Promise.<*|null>} - value stored at location or null
   * @memberof SequelizeDbMeta
   */
  getOrNull: (key, transaction) =>
    _globalInstance.getOrNull(key, transaction),

  /**
   * @desc gets value stored at storage or throws an error
   * @param {string} key - target key
   * @param {*=} transaction - an instance to sequelize transaction object
   * @return {Promise.<*>} - value stroed at location
   * @memberof SequelizeDbMeta
   */
  get: (key, transaction) =>
    _globalInstance.get(key, transaction),

  /**
   * @desc sets value in storage
   * @param {string} key - target key
   * @param {*} value - new value to set
   * @param {*=} transaction - an instance to sequelize transaction object
   * @return {Promise} - resolves when value is set
   * @memberof SequelizeDbMeta
   */
  put: (key, value, transaction) =>
    _globalInstance.put(key, value, transaction),

  /**
   * @desc merge previous value of key into new value
   * @param {string} key - key to requested value
   * @param {*} value - any javascript object to store
   * @param {*=} data - additional optional data to write
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise} - resolve when value is created
   */
  assign: (key, value, data, transaction) =>
    _globalInstance.assign(key, value, data, transaction),

  /**
   * @desc clears items in storage
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise} - resolves when storage is cleared
   * @memberof SequelizeDbMeta
   */
  clear: transaction =>
    _globalInstance.clear(transaction),

  /**
   * @desc count number of items in storage
   * @param {string=} pattern - optional wildcard pattern string
   * @param {*=} where - optional additions to where clause
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise.<number>} - number of items
   * @memberof SequelizeDbMeta
   */
  count: (pattern, where, transaction) =>
    _globalInstance.count(pattern, where, transaction),

  /**
   * @desc list items in storage
   * @param {number=} start - offset to start listing
   * @param {number=} length - number of items to look for
   * @param {string=} pattern - optional wildcard pattern string
   * @param {*=} where - optional additions to where clause
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise.<Array.<*> >} - items
   * @memberof SequelizeDbMeta
   */
  all: (start, length, pattern, where, transaction) =>
    _globalInstance.all(start, length, pattern, where, transaction),

  /**
   * @desc sets expiraton time on key
   * @param {string} key - target key
   * @param {number} time - time in milliseconds
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise} - resolves when expiration is set
   * @memberof SequelizeDbMeta
   */
  expire: (key, time, transaction) =>
    _globalInstance.expire(key, time, transaction),

  /**
   * @desc shows whether a key exists
   * @param {string} key - key to look for
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise.<boolean>} - true if item exists
   */
  has: (key, transaction) =>
    _globalInstance.has(key, transaction),

  /**
   * @desc deletes a key and shows whether it actually existed
   * @param {string} key - key to look for
   * @param {*=} transaction - optional sequelize transaction object
   * @return {Promise.<boolean>} - true if item existed
   */
  delete: (key, transaction) =>
    _globalInstance.delete(key, transaction),

  /**
   * @desc re-schedule expired items clearer cron daemon
   * @param {string=} schedule - a node-cron compatible cron tab spec.
   * default is every 20 minutes
   * @memberof SequelizeDbMeta
   */
  monitor: schedule => {
    _globalInstance.monitor(schedule);
  },

  /**
   * @desc collects garbage and removes expired items
   * @return {Promise} - resolves when gc is done
   * @memberof SequelizeDbMeta
   */
  gc: () => _globalInstance.gc(),

  /**
   * @desc gets table name used for storing meta data
   * @return {string} - table name used for meta data
   * @memberof SequelizeDbMeta
   */
  tableName: () => _globalInstance.tableName,

  /**
   * @desc sets prefix for keys accessed by meta database
   * @param {string} newPrefix - prefix to set on keys
   * @return {SequelizeDbMetaInstance} - a new instance with prefix set
   * @memberof SequelizeDbMeta
   */
  prefix: newPrefix => _globalInstance.prefix(newPrefix),

  /**
   * @desc this method is used in mocha test cases to prove
   * memory-leak safetiness.
   * @return {number} - number of garbage collections detected in CRON job
   * @memberof SequelizeDbMeta
   * @private
   */
  __getDestroyCounter: () => __destroyCounter
});
