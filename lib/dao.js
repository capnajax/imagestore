'use strict';

const c = require('./config');
const debug = require('@capnajax/debug')('imagestore:dao')
const fs = require('fs').promises;
const { Pool } = require('pg');
const _ = require('lodash');

let dataProperties;

// data that is cached. Each cache contains a `ttl` -- number of seconds, and
// `data` the data in the cache. Each data object's value has `data`, containing
// the actual cached value, and a `date`
let caches = {
  camerasExist: {
    ttl: 60000,
    data: {}
  }
};

const _getClient_poolFn = _.memoize(async () => { 
  debug('[_getClient_poolFn] initializing pool');
  try {
    const password = (await fs.readFile('/mnt/creds/pgpassword')).toString().replace(/\s/g, '');
    const pgClientOpts = {
      host: c('PGHOST'),
      port: Number.parseInt(c('PGPORT')),
      user: c('PGUSER'),
      password,
      database: c('PGDATABASE'),
    };

    debug('[_getClient_poolFn] pgClientOpts:');
    let redacted = _.clone(pgClientOpts);
//    redacted.password && (redacted.password = '*'.repeat(redacted.password.length));
    debug(redacted);

    let pool = new Pool(pgClientOpts);

    await _populateDataProperties(pool);

    return pool;
 
  } catch (reason) {
    // this failure should cause a crash.
    console.log("[_getClient_poolFn] FATAL: Failure getting connect pool");
    console.log(reason);
    process.exit(1);
  }
});

async function _getClient() {

  // remember this memoizes a promise
  let pool = await _getClient_poolFn();

  // returns a client
  return await pool.connect();

}

/**
 *  @method _populateDataProperties
 *  Internal method to create a connection to the database and update the
 *  metadata, and returns a promise when it's done. Only used by `_getClient()`
 *  @param {Pool} the connection pool to use
 */
function _populateDataProperties(pool) {
  debug('[_populateDataProperties] called');
  return new Promise((resolve, reject) => {
    let schemaVersion;
    let mediaTypes = {};
    let tags = {};
    let cameras = {};
    let camerasById = {};
    let thumbnailSpecs = {};
    let cameraTags;
    let thisClient;

    pool.connect()
    .then(client => {
      thisClient = client;
      return Promise.all([
        // get the current schema version
        client.query('SELECT config_value FROM config WHERE config_name = $1', ['schema_version'])
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
            cameraTags = _.clone(res.rows);
          }),
        client.query('SELECT * FROM thumbnail_spec')
          .then(res => {
            res.rows.forEach(row => {
              thumbnailSpecs[row.id] = {description: row.descr};
            });
          })
      ])
      .then(() => { resolve(); })
      .catch((reason) => {
        // this failure should cause a crash.
        console.log("[dao] FATAL: Failure getting dataProperties")
        console.log(reason);
        process.exit(1);
      })
      .finally(() => {
        thisClient.release();
      });
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
      debug('[_populateDataProperties] dataProperties:');
      debug(dataProperties);
    });
  });
}

/**
 *  @method cacheGet
 *  Checks the cache for a property
 *  @param {string} cache
 *  @param {*} key
 *  @param {function} [ifNotFn] calls this function if the cached value does not
 *    exist.
 *  @return {*} if an `ifNotFn` is provided, the cache value. If not, an object
 *    indicating if the value exists, and what it is.
 */
function cacheGet(cache, key, ifNotFn) {
  let result = {exists: false, cache, key}
  if (_.has(caches, cache) && _.has(caches[cache].data, key)) {
    let time = Date.now();
    if (caches[cache].ttl + caches[cache].data[key].date > time) {
      result.exists = true;
      result.value = caches[cache].data[key].value;
    } else {
      delete caches[cache].data[key];
    }
  }
  if (!result.exists && ifNotFn) {
    result = ifNotFn();
    Promise.resolve(result)
    .then(value => {
      cachePut(cache, key, value);
    });
  }
  return result;
}

/**
 *  @method cacheInvalidate
 *  Invalidates an entire cache
 *  @param {string} cache
 */
function cacheInvalidate(cache) {
  if (_.has(caches, cache)) {
    caches[cache].data = {};
  } else {
    debug('[cacheInvalidate] cache', cache, 'does not exist. Ignoring.');
  }
}

/**
 *  @method cachePut
 *  Puts an object in the cache
 *  @param {string} cache
 *  @param {*} key
 *  @param {*} value 
 */
function cachePut(cache, key, value) {
  if (_.has(caches, cache)) {
    caches[cache].data[key] = {
      date: Date.now(),
      value
    };
  } else {
    debug('[cachePut] cache', cache, 'does not exist. Cannot store.');
  }
}

/**
 *  @method cameraExists
 *  @param {string} camera the name of the camera
 *  @return {boolean} true if the camera exists, false if not. 
 */
async function cameraExists(camera) {
  return cacheGet(
    'camerasExist',
    camera,
    async () => {
      let client = await _getClient();
      try {
        return client.query(
          'SELECT * FROM camera WHERE camera_name = $1',
          [ camera
          ])
          .then(res => {
            let result = (res.rows.length > 0);
            cachePut('camerasExist', camera, result);
            return result;
          });
      } finally {
        client.release();
      }
    });
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
async function createPhoto(img) {
  let client = await _getClient();
  let mediaType = img.mediaType || 'image/jpeg';

  try {
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
  } finally {
    client.release();
  }
}

/**
 *  @method getThumbnailSpecs
 */
function getThumbnailSpecs() {
  return _.clone(dataProperties.thumbnailSpecs);
}

_getClient().then(client => client.release());

module.exports = {
  cameraExists,
  createPhoto,
  getThumbnailSpecs
};
