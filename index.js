"use strict";

const task = require('xcane').task;
let Meta = null;

class SequelizeDbMeta {
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
    });
  }

  static _get(key) {
    return task.spawn(function* () {
      try {
        return yield Meta.findOne({
          where: {
            key: key
          }
        });
      } catch (err) {
        console.log('ERRRRRRR:', err);
        throw err;
      }
    });
  }
}
