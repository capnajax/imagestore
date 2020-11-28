
const c = require('../lib/config');
const fs = require('fs').promises;
const path = require('path');
const randomstring = require('randomstring');
const redis = require('redis');
const util = require('util');
const _ = require('lodash');

const redisClient = redis.createClient();

let redisOps = {};
for (let i of ['exists', 'hset', 'xadd']) {
  console.log('Promisifying', i);
  redisOps[i] = util.promisify(redisClient[i]).bind(redisClient);
}

let existingCameras = new Set();
let existingDirs = new Set();

/**
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
  storeImage
}