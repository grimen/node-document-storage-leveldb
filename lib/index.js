require('sugar')
var util = require('util');

// HACK: ...until Node.js `require` supports `instanceof` on modules loaded more than once. (bug in Node.js)
var Storage = global.NodeDocumentStorage || (global.NodeDocumentStorage = require('node-document-storage'));

// -----------------------
//  DOCS
// --------------------
//  - https://github.com/rvagg/node-levelup

// -----------------------
//  Constructor
// --------------------

// new LevelDB ();
// new LevelDB (options);
// new LevelDB (url);
// new LevelDB (url, options);
function LevelDB () {
  var self = this;

  self.klass = LevelDB;
  self.klass.super_.apply(self, arguments);
}

util.inherits(LevelDB, Storage);

// -----------------------
//  Class
// --------------------

LevelDB.defaults = {
  url: process.env.LEVELDB_URL || 'file:///tmp/{db}-{env}'.assign({db: 'default', env: (process.env.NODE_ENV || 'development')}),
  options: {
    server: {
      mode: 0777,
      extension: '.ldb' // REVIEW: Move into connection URL?
    },
    client: {
      mode: 'a+'
    }
  }
};

LevelDB.url = LevelDB.defaults.url;
LevelDB.options = LevelDB.defaults.options;

LevelDB.reset = Storage.reset;

// -----------------------
//  Instance
// --------------------

LevelDB.prototype.connect = function() {
  var self = this;

  self._connect(function() {
    var fs = require('node-fs');
    var path = require('path');
    var levelup = require('levelup');

    var db = self.resource().db + self.resource().ext;

    self.client = (process.levelup && process.levelup[db]) || levelup(db);

    process.levelup = process.levelup || {};
    process.levelup[db] = self.client;

    fs.mkdir(path.dirname(self.resource().db), self.options.server.mode, true, function(err) {
      self.emit('ready', err);
    });
  });
};

// #set (key, value, [options], callback)
// #set (keys, values, [options], callback)
LevelDB.prototype.set = function() {
  var self = this;

  // TODO: Use `batch()` API

  self._set(arguments, function(key_values, options, done, next) {
    key_values.each(function(key, value) {
      var resource = self.resource(key);
      var db = self.resource().db + self.resource().ext;

      // NOTE: Need connection pool to handle this - only one connection allowed.

      self.client = (process.levelup && process.levelup[db]) || levelup(db);

      process.levelup = process.levelup || {};
      process.levelup[db] = self.client;

      self.client.put(resource.key, value, function(error, response) {
        next(key, error, !error, response);
      });
    });
  });
};

// #get (key, [options], callback)
// #get (keys, [options], callback)
LevelDB.prototype.get = function() {
  var self = this;

  // TODO: Use `batch()` API

  self._get(arguments, function(keys, options, done, next) {
    keys.each(function(key) {
      var resource = self.resource(key);
      var db = self.resource().db + self.resource().ext;

      // NOTE: Need connection pool to handle this - only one connection allowed.

      self.client = (process.levelup && process.levelup[db]) || levelup(db);

      process.levelup = process.levelup || {};
      process.levelup[db] = self.client;

      self.client.get(resource.key, function(error, response) {
        next(key, error, response || null, response);
      });
    });
  });
};

// #del (key, [options], callback)
// #del (keys, [options], callback)
LevelDB.prototype.del = function() {
  var self = this;

  // TODO: Use `batch()` API

  self._del(arguments, function(keys, options, done, next) {
    keys.each(function(key) {
      var resource = self.resource(key);
      var db = self.resource().db + self.resource().ext;

      // NOTE: Need connection pool to handle this - only one connection allowed.

      self.client = (process.levelup && process.levelup[db]) || levelup(db);

      process.levelup = process.levelup || {};
      process.levelup[db] = self.client;

      self.client.get(resource.key, function(_, before) {
        self.client.del(resource.key, function(error, response) {
          self.client.get(resource.key, function(_, after) {
            next(key, error, !!(before && before !== after), response);
          });
        });
      });
    });
  });
};

// #exists (key, [options], callback)
// #exists (keys, [options], callback)
LevelDB.prototype.exists = function() {
  var self = this;

  // TODO: Use `batch()` API

  self._exists(arguments, function(keys, options, done, next) {
    keys.each(function(key) {
      var resource = self.resource(key);
      var db = self.resource().db + self.resource().ext;

      // NOTE: Need connection pool to handle this - only one connection allowed.

      self.client = (process.levelup && process.levelup[db]) || levelup(db);

      process.levelup = process.levelup || {};
      process.levelup[db] = self.client;

      self.client.get(resource.key, function(error, response) {
        next(key, error, !!response, response);
      });
    });
  });
};

// #end ()
LevelDB.prototype.end = function() {
  var self = this;

  if (self.client) {
    self.client.close && self.client.close();
  }
};

LevelDB.prototype.pack = JSON.stringify;

LevelDB.prototype.unpack = JSON.parse;

// -----------------------
//  Export
// --------------------

module.exports = LevelDB;
