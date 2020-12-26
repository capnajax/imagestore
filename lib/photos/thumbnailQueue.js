'use strict';

const c = require('../config');
const catalog = require('../catalog');
const dao = require('../dao');
const debug = require('@capnajax/debug')('imagestore:thumbnailQueue');
const fs = require('fs').promises;
const http = require('http');
const path = require('path');
const _ = require('lodash');
const { SSL_OP_CRYPTOPRO_TLSEXT_BUG } = require('constants');

// tasks yet to be started
let workQueue = [];
let pendingQueue = 0; // number of jobs awaiting validation ebfore queueing
let itemsInProcess = {};

const maxThreads = c('max_image_threads');
const maxQueue = c('max_image_queue');
const imageProcessorService = c('image_processor_service');
const imageProcessorServicePort = c('image_processor_service_port');

const QUEUE_OPEN = 'open';
const QUEUE_PAUSED = 'paused';

// all file system operations except writing thumbnails to disk
// are in this thread

/**
 *  @method checkQueue
 *  Tests if another thread can be started. If so, start it.
 */
function checkQueue() {
  if (debug.enabled) {
    debug('[checkQueue] called');
    debug('[checkQueue] workQueue:');
    debug(workQueue);
    debug('[checkQueue] itemsInProcess (', Object.keys(itemsInProcess).length,')');
    debug(itemsInProcess);
    debug('[checkQueue] maxThreads ==', maxThreads);
  }

  if (workQueue.length > 0) {
    if (Object.keys(itemsInProcess).length < maxThreads) {
      debug('[checkQueue] processing workItem:')
      let workItem = workQueue.shift();
      debug(workItem);
      processImage(workItem);
      checkQueue();
    }
  }
}

async function processImage(workItem, mediaType = 'image/jpeg') {

  debug('[processImage] called -- workItem:');
  debug(workItem);

  let thumbnailSpecs = dao.getThumbnailSpecs();
  let operationDefinition = { pathname: workItem.pathname, commands: [] };

  debug('[processImage] thumbnailSpecs:');
  debug(thumbnailSpecs);



  // TODO why are there no jobs in the request




  let requestImageProcess = function(operationDefinitionBuffer) {
    let responseBody = '';
    let req = http.request({
      port: imageProcessorServicePort,
      host: imageProcessorService,
      hostname: 'imageprocessor',
      path: '/job',
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        'Content-Length': operationDefinitionBuffer.length
      }
    }, (res) => {

      res.on('data', function (chunk) {
        debug('[processImage] on data got chunk');
        debug(chunk);
        responseBody += chunk;
      });
      res.on('end', async function() {
        debug('[processImage] on end called, status:', res.statusCode);
        debug('[processImage] on end operationDefinitionBuffer:',
          operationDefinitionBuffer.toString());
        debug('[processImage] on end responseBody:', responseBody);

        // check for error statuses

        // handle response from the image processor
        if (res.statusCode === 200) {
          message = JSON.stringify(responseBody);
          let img = {
            camera: workItem.camera,
            date: workItem.date,
            mediaType,
            filename: message.pathname,
            thumbnails: _.map(message.commands, cmd => {
              return {
                filename: path.join(message.outputDir, cmd.filename),
                spec: cmd.specname
              }
            })
          }
          await dao.createPhoto(img);
          await catalog.removeImage(workItem.key);
        }

        delete itemsInProcess[workItem.key];
        checkQueue();
      });
    });

    req.write(operationDefinitionBuffer);
    req.end();

  };

  itemsInProcess[workItem.key] = workItem;

  debug('[processImage] thumbnailSpecs ==', thumbnailSpecs);

  for (let thumbSpecId of Object.keys(thumbnailSpecs)) {
    debug('[processImage] thumbSpecId ==', thumbSpecId, ', thumbnailSpecs[thumbSpecId] ==', thumbnailSpecs[thumbSpecId]);
    let thumbSpec = thumbnailSpecs[thumbSpecId];
    let command = _.extend({filename: thumbSpec.name}, thumbSpec.spec);
    debug('[processImage] command ==', command);
    operationDefinition.commands.push(command);
  }
  debug('[processImage] commands:');
  debug(operationDefinition.commands);



  // TODO commands is not right -- {filename: undefined} -- should there be more information in this request?


  

  requestImageProcess(Buffer.from(JSON.stringify(operationDefinition)));
}

/**
 * @method queueJob 
 * Put a job in the queue for processing.
 * @param {object} metadata image metadata to use to build job.
 * @param {string} metadata.key unique identifier for the image.
 * @param {string} metadata.pathname the path of the imagefile to process
 * @param {string} metadata.camera the name of the camera that took the photo
 * @param {Date} metadata.date the date the photo was taken
 * @throws when a job is invalid 
 */
async function queueJob(metadata) {
  pendingQueue++;
  try {
    debug('[queueJob] called with metadata ==', metadata);
    // validate job before enqueueing it
    let errors = [];
    debug('[queueJob] testing pathname', metadata.pathname);
    if (metadata.pathname) {
      debug('[queueJob] stat on pathname', metadata.pathname);
      let stat = await fs.stat(metadata.pathname);
      debug('[queueJob] stat', stat);
      stat.isFile() || errors.push(`File "${metadata.pathname}" does not ` +
        'exist or is not a regular file.');
    } else {
      errors.push('Pathname not specified.');
    }
    debug('[queueJob] testing camera', metadata.camera);
    if (metadata.camera) {
      // test if the camera exists
      await dao.cameraExists(metadata.camera) ||
        errors.push(`Camera "${metadata.camera}" unknown`);
    } else {
      errors.push('Camera not specified.');
    }
    debug('[queueJob] testing date', metadata.date);
    if (!_.isDate(metadata.date)) {
      errors.push('Image date not specified.');
    }

    if (errors.length > 0) {
      debug('[queueJob] erros in job request:', errors);
      throw { message: 'errors requesting job', details: errors };
    }
    debug('[queueJob] job is valid');

    workQueue.push(metadata);
  } finally {
    debug('[queueJob] finally');
    pendingQueue--;
    debug('[queueJob] finally checking queue');
    checkQueue();
  }
}

/**
 * @method queueStatus
 * Gets the status of a queue. Can be QUEUE_PAUSED or QUEUE_OPEN. Jobs should
 * not be enqueued when the queue is paused. Note `queueJob` will accept jobs
 * regardless of status but it's best to ask first.
 */
function queueStatus() {  
  if (workQueue.length + pendingQueue >= maxQueue) {
    debug('[queueStatus] maxQueue exceeded, QUEUE_PAUSED,', workQueue.length,
      '+', pendingQueue, '>=', maxQueue);
    return QUEUE_PAUSED;
  } else {
    debug('[queueStatus] QUEUE_OPEN,', workQueue.length, '+', pendingQueue,
      '<', maxQueue);
    return QUEUE_OPEN;
  }
}

module.exports = {
  queueStatus,
  queueJob,
  QUEUE_OPEN,
  QUEUE_PAUSED
};
