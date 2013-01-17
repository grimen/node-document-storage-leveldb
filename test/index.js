
var Storage = require('node-document-storage');

// -----------------------
//  Test
// --------------------

module.exports = Storage.Spec('LevelDB', {
  module: require('..'),
  engine: require('levelup'),
  db: 'default-test',
  default_url: 'file:///tmp/default-test',
  authorized_url: undefined,
  unauthorized_url: undefined,
  client: {
    get: function(db, type, id, callback) {
      var key = [type, id].join('/');
      var db = '/tmp/' + db + '.ldb';

      // NOTE: Need connection pool to handle this - only one connection allowed.

      var client = (process.levelup && process.levelup[db]) || require('levelup')(db);

      process.levelup = process.levelup || {};
      process.levelup[db] = process.levelup[db] || client;

      client.open(function() {
        client.get(key, function(err, res) {
          callback(err, res || null);
        });
      });
    },

    set: function(db, type, id, data, callback) {
      var key = [type, id].join('/');
      var db = '/tmp/' + db + '.ldb';

      var client = (process.levelup && process.levelup[db]) || require('levelup')(db);

      process.levelup = process.levelup || {};
      process.levelup[db] = process.levelup[db] || client;

      client.put(key, data, function(err, res) {
         callback(err, res);
      });
    },

    del: function(db, type, id, callback) {
      var key = [type, id].join('/');
      var db = '/tmp/' + db + '.ldb';

      var client = (process.levelup && process.levelup[db]) || require('levelup')(db);

      process.levelup = process.levelup || {};
      process.levelup[db] = process.levelup[db] || client;

      client.del(key, function(err, res) {
         callback(err, !err);
      });
    },

    exists: function(db, type, id, callback) {
      var key = [type, id].join('/');
      var db = '/tmp/' + db + '.ldb';

      var client = (process.levelup && process.levelup[db]) || require('levelup')(db);

      process.levelup = process.levelup || {};
      process.levelup[db] = process.levelup[db] || client;

      client.get(key, function(err, res) {
         callback(err, !!res);
      });
    }
  }
});
