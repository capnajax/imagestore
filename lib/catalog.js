'use strict';

/**
 * @module catalog
 * This is a catalog of images that have not yet been processed.
 */

const c = require('../lib/config');
const debug = require('@capnajax/debug')('imagestore:catalog');
const fs = require('fs').promises;
const path = require('path');
const queue = require('./photos/thumbnailQueue');
const randomstring = require('randomstring');
const redis = require('redis');
const timers = require('timers');
const util = require('util');
const _ = require('lodash');

const redisClient = redis.createClient();

let redisOps = {};
for (let i of ['exists', 'keys', 'hgetall', 'hkeys', 'hset', 'scan', 'xadd']) {
  console.log('Promisifying', i);
  redisOps[i] = util.promisify(redisClient[i]).bind(redisClient);
}

let existingCameras = new Set();
let existingDirs = new Set();

let _loadImagesCursor = 0;

/**
 * @method correctImages
 * This is a script run to validate the catalog and make corrections as
 * necessary.
 */
async function correctImages() {
  debug('[correctImages] called');

  const moduleTag = '[catalog:correctImages]';
  const reportCorrections = () => {
    console.log(moduleTag, 'CORRECTIONS REPORT:');
    console.log(moduleTag, `running time: ${
      (Date.now() - startTime)/1000}, records checked: ${
      corrections.checked}`);
    console.log(moduleTag, `corrections made: ${
      corrections.records} records, ${corrections.filenames} filenames`);
    corrections.errors && console.log(moduleTag, 'errors reported:',
      corrections.errors);
  };

  let corrections = {checked: 0, records: 0, filenames: 0, errors: 0};
  let startTime = Date.now();
  let correctionsReportInterval = timers.setInterval(() => {
    reportCorrections();
  }, 5000);
  debug('[correctImages] correctionsReportInterval ==',
    correctionsReportInterval);

  let pathsSeen = {};

  //
  //  Test for bad filenames -- early development created many filenames with
  //  the extension ".undefined" instead of ".jpg"; this corrects those records.
  //
  let fixFilenames = async (metadataKey, image) => {
    corrections.checked++;
    if (image.path.match(/.*\.undefined$/)) {
      const modulePrefix = `${moduleTag}      --> ${image.metadataKey}`;
      console.log(
        `${moduleTag} correcting ${image.metadataKey} path ${image.path}`);
      let imageFilestats = await fs.stat(image.path).catch(() => {return null;});
      let newPathname = image.path.replace(/\.undefined$/, '.jpg');
      if (imageFilestats && imageFilestats.isFile()) {
        console.log(`${modulePrefix} renaming file to ${newPathname}`);
        await fs.rename(image.path, newPathname);
        corrections.filenames++;
      }
      image.path = newPathname;
      image.filename = path.basename(newPathname);
      let newStat = await fs.stat(newPathname).catch(() => {return null;});
      if (!(newStat && newStat.isFile())) {
        console.log(`${modulePrefix} ERROR ${newPathname} does not exist`);
        corrections.errors++;
      }
      await redisOps.hset(metadataKey, _.flatten(_.toPairs(image)));
      corrections.records++;
    }    
  };

  //
  //  Ensures there is only one metadata entry per image file
  //
  let checkUnique = (metadataKey, image) => {
    if (_.has(pathsSeen, image.path)) {
      pathsSeen[image.path].push(metadataKey);
      console.log(`${moduleTag} duplicate path ${image.path} in ${
        pathsSeen[image.path]}`);
      corrections.errors++;
    } else {
      pathsSeen[image.path] = [metadataKey];
    }
  }

  let cursor = '0';
  do {
    let scanOp = await redisOps.scan(
      cursor,
      'MATCH', 'image:meta:*');

    cursor = scanOp[0];
    let keys = scanOp[1];

    for (let key of keys) {
      let image = await redisOps.hgetall(key);
      await fixFilenames(key, image);
      checkUnique(key, image);
    };

  } while (cursor !== '0'); 

  timers.clearInterval(correctionsReportInterval);
  reportCorrections();
  console.log(moduleTag, 'CORRECTIONS COMPLETE');
}

/**
 * @method importImages
 * Queues images for conversion and adding to the database.
 */
async function importImages() {
  debug('[importImages] called');

  let importCore = async () => {
    debug('[importImages] importCore called');

    if (queue.queueStatus() === queue.QUEUE_OPEN) {
      let images = await loadImages();
      if (images.length > 0) {
        debug('[importImages] importCore queueing', images.length, 'images');

        images.forEach(image => {

          let dateNum = image.event.replace(/-.*/, '');
          let date = new Date(Number.parseInt(dateNum));
          debug('[importImages] image.event ==', image.event);
          debug('[importImages] dateNum ==', dateNum);
          debug('[importImages] date ==', date);

          let imageStreamFields = image.stream.split(':');
          let camera = imageStreamFields[2];
          let queueable = {
            key: image.metadataKey,
            pathname: image.path,
            camera,
            date
          }
          debug('[importImages] importCore queueing image', queueable);
          queue.queueJob(queueable);
          
        })

      } else {
        debug('[importImages] importCore no images waiting.');
      }

    } else {
      debug('[importImages] importCord too many images waiting. Not queueing more');
    }
  } 
  setInterval(() => {
    // set this to run every half-second, block if it's already running, but
    // don't wait on a run over 5 seconds long. A failure will look like an 
    // interval is running long.
    if (_isImportingImages &&
      _isImportingImages + c('max_import_interval') < Date.now()) {
      debug('[importingImages] import page timeout');
      _isImportingImages = false;
    }
    if (!_isImportingImages) {
      _isImportingImages = Date.now();
      importCore();
    }
    _isImportingImages = false;

  }, c('min_import_interval'));
}
let _isImportingImages = false;



/**
 * @method loadImages
 * Load a single image from storage with metadata
 * @param {string} metadataKey the key of the image
 */
async function loadImage(metadataKey) {
  let result = await redisOps.hgetall(metadataKey);
  return result;
}

/**
 * @method loadImages
 * Load some images from storage with metadata.
 * @param {string} [metadataKey] if provided, load the image specified by the
 *  metadata key. To get an image from a specific camera, use a wildcard or
 *  name the camera, and it'll get a random image from that camera. If no
 *  key is specified, it'll get a random image.
 * @param {number} [numImages] cap the number of images loaded. If not
 *  provided, it caps the number of images at a value based on the number
 *  at `c('max_image_queue')`.
 */
async function loadImages(metadataKey, numImages) {

  // quick validation to determine that record loaded by redis.scan is, in
  // fact, an image.
  let validateImage = (image) => {
    return !_.isNil(image.stream);
  }

  if (_.isNil(numImages)) {
    numImages = c('max_image_queue')
  }

  if (_.isNumber(metadataKey)) {
    numImages = metadataKey;
    metadataKey = '';
  }

  if (!metadataKey) {
    metadataKey = 'image:meta:*';
  }

  if (!metadataKey.startsWith('image:meta:')) {
    metadataKey = `image:meta:${metadataKey}`;
  }

  let scanOp = await redisOps.scan(_loadImagesCursor, 'MATCH', metadataKey, 'COUNT', numImages);
  let results = [];
  let promises = _.map(scanOp[1], key => {
    return loadImage(key)
      .then(image => {
        if (validateImage(image)) {
          results.push(image);
        }
      });
  });
  await Promise.all(promises);
  return results;
}

/**
 * @method removeImage
 * Remove an image from catalog
 * @param {string} metadataKey 
 */
async function removeImage(metadataKey) {
  await redisOps.del(metadataKey);
}

/**
 * @method storeImage
 * Accept an image for storage
 * @param {string} version the version of the camera app
 * @param {object} metadata image options
 * @param {string} metadata.version the version of the camera app
 * @param {string} metadata.camera the camera's name
 * @param {string} [metadata.format] the image format (default 'jpg')
 * @param {Buffer} the image data to store
 */
async function storeImage(metadata, imageData) {

  console.log("storeImage", metadata);

  let camera = metadata.camera;
  let version = metadata.version;
  metadata.format || (metadata.format = 'jpg');
  if (!existingCameras.has(camera)) {
      existingCameras.add(camera);
  }

  let metadataKey;
  do {
    metadataKey = `image:meta:${camera}:${randomstring.generate(16)}`
  } while(0 !== await redisOps.exists(metadataKey));

  let eventStream = `image:stream:${camera}`;
  let key = await redisOps.xadd(eventStream, '*', 'v', version, 'metadata', metadataKey);
  let filename = `${key}.${metadata.ext}`;
  let filedir = path.join(c('images-path'), camera);
  let filepath = path.join(filedir, filename);

  // save file
  console.log('checking for dir', filedir);
  if (!existingDirs.has(filedir)) {
    console.log(' --> dir', filedir, 'not previously encountered');
    await fs.mkdir(filedir, {recursive: true});
    console.log(' --> dir', filedir, 'created');
    existingDirs.add(filedir);
  }
  await fs.writeFile(filepath, imageData);
  console.log("Wrote file" + filepath);

  await redisOps.hset(metadataKey, 
  _.flatten(_.toPairs({
      v: version,
      filename,
      path: filepath,
      metadataKey,
      stream: eventStream,
      event: key
  })));
  // done
}

module.exports = {
  loadImage,
  loadImages,
  removeImage,
  storeImage
}

// automatically run correctImages and begin import processes
Promise.resolve()
.then(() => {
  if (!process.env['SKIP_CATALOG_VALIDATION']) {
    // return correctImages();
  }
})
.then(() => {
  importImages();
});
