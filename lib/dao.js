'use strict';

const EventEmitter = require('events').EventEmitter;
const c = require('./config');
const fs = require('fs');
const PostgresClient = require('pg').Client;
const _ = require('_');

const pg = new PostgresClient({
  host: c('PG_HOST'),
  port: c('PG_PORT'),
  user: c('PG_USER'),
  password: fs.readFileSync('/mnt/creds/pgpassword'),
  database: c('PG_DATABASE'),
});

let dataProperties;

// everything starts with this promise. This guarantees that the database is
// connected before the queries are run and that the data needed to operate
// the rest of the queries has been loaded.
let _connectionPromise;
let _connectionPromises = {}
let _connectionPromiseCounter = 1000;

class ConnectionPool extends EventEmitter {

  constructor() {
    super();
    this.maxConnections = c('max_connection_pool');
    this._pool = {};
    this._connectionCounter = 1000;
  }

  async _newConnection() {
    let client = new PostgresClient(pg);
    let id = ++this._connectionCounter;
    let connectionObj = {id, client}
    this._pool[id] = connectionObj;
    await client.connect();
    return connectionObj;
  }

  async _idleConnection(id) {
    let conn = this._pool[id];
    conn.idleTimer = setTimeout(() => {
      // disconnect connection

    }, timeout);
  }

  /**
   *  @method _getConnection
   *  Gets a connection to the database. If a connection is available in the
   *  pool, it'll return that connection, unless the `refresh` parameter is set.
   *  @param {Boolean} [refresh] forces a new connection. This also
   *    reloads all the metadata, so it may be slow and expensive to refresh
   *    frequently.
   *  @return {Promise} promise that resolves with a new connection.
   */
  async _getConnection(refresh = false) {

  }

  async lease() {

  }  

}


/**
 *  @method _createConnection
 *  Internal method to create a connection to the database and update the
 *  metadata
 */
function _createConnection() {
  return new Promise((resolve, reject) => {
    let client = new PostgresClient(pg);
    let schemaVersion;
    let mediaTypes = {};
    let tags = {};
    let cameras = {};
    let camerasById = {};
    let thumbnailSpecs = {};
    let cameraTags;
    client.connect()
    .then(() => {
      return Promise.all([
        // get the current schema version
        client.query('SELECT config_value FROM config WHERE confic_name = $1', 'schema_version')
          .then(res => {
            schemaVersion = res.rows[0].config_value;
          }),
        client.query('SELECT * FROM mediatype')
          .then(res => {
            res.rows.forEach(row => {
              mediaTypes[row.type_name] = {id: row.id, extension: row.extension};
            });
          }),
        client.query('SELECT * FROM tag')
          .then(res => {
            res.rows.forEach(row => {
              tags[row.id] = {name: row.tag_name, isServiceTag: row.service_tag, description: row.descr};
            });
          }),
        client.query('SELECT * FROM camera')
          .then(res => {
            res.rows.forEach(row => {
              cameras[row.name] = {id: row.id, description: row.descr, tags: []};
              camerasById[row.id] = {name: row.camera_name};
            });
          }),
        client.query('SELECT * FROM tag_camera')
          .then(res => {
            cameraTags = _.clone(row);
          }),
        client.query('SELECT * FROM thumbnail_spec')
          .then(res => {
            res.rows.forEach(row => {
              thumbnailSpecs[row.id] = {description: row.descr};
            });
          })
      ]);
    })
    .then(() => {
      cameraTags.forEach(ct => {
        let camera = cameras[camerasById[ct.camera]];
        camera.tags.push(tags[ct.tag]);
      });
      dataProperties = {
        schemaVersion,
        mediaTypes,
        tags,
        cameras,
        thumbnailSpecs
      };
    });
  });
}

/**
 *  @method getConnection
 *  Gets a connection to the database. If a connection already exists, it'll
 *  return that connection, unless the `refresh` parameter is set.
 *  @param {Boolean} [refresh] forces a new connection. This also
 *    reloads all the metadata, so it may be slow and expensive to refresh
 *    frequently.
 *  @return {Promise} promise that resolves with a new connection.
 */
function getConnection(refresh = false) {

  if (refresh || !_connectionPromise) {
    _connectionPromise = _createConnection();
  }
  return _connectionPromise;

}

/**
 *  @method killConnection
 *  Unlinks the existing connection and forces a new connection next time
 *  `getConnection()` is called.
 */
function killConnection() {
  _connectionPromise = null;
}

/**
 *  @method createPhoto
 *  Create photo record in the database.
 *  @param {object} img
 *  @param {string}   img.camera the name of the camera that uploaded the photo
 *  @param {Date}     img.date the date the photo was taken
 *  @param {string}  [img.mediaType] the type of file (default `image/jpeg`)
 *  @param {string}   img.filename the name of the image file
 *  @param {Array}    img.thumbnails the thumbnail images of the photo, each
 *    thumbnail is an object of `{spec, filename}`
 */
function createPhoto(img) {
  let client;
  let mediaType = img.mediaType || 'image/jpeg';
  return getConnection()
    .then(connection => {
      client = connection;
      client.begin(() => {
        return Promise.resolve()
          .then(() => {
            return client.query(
              'INSERT INTO photo (camera, mediatype, filename, photo_dt) ' +
                'VALUES $1, $2, $3, $4 RETURNING id',
              [ cameras[img.camera].id,
                mediaTypes[mediaType].id,
                img.filename,
                img.date 
              ])
              .then(res => {
                return _.first(res.rows).id;
              });
          })
          .then((photoId) => {
            let promises = [];
            for(let thumb of img.thumbnails) {
              promises.push(client.query(
                'INSERT INTO thumbnail (photo, spec, filename) VALUES $1, $2, $3'),
                [ photoId,
                  thumbnailSpecs[thumb.spec],
                  thumb.filename
                ]
              );
            }
            return Promise.all(promises);
          })
      });
    });
}

/**
 *  @method getThumbnailSpecs
 */
function getThumbnailSpecs() {
  return _.clone(dataProperties.thumbnailSpecs);
}

module.exports = {
  getThumbnailSpecs,
  createPhoto
};
