'use strict';

const dao = require('../dao');
const debug = require('@capnajax/debug')('imagestore:thumbnailQueue');
const fs = require('fs').promises;
const http = require('http');
const path = require('path');

// tasks yet to be started
let workQueue = [];

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
  if (workQueue.length > 0) {
    if (Object.keys().length < maxThreads) {
      let workItem = workQueue.shift();
      processImage(workItem.pathname, workItem.camera, workItem.date);
      checkQueue();
    }
  }
}

async function processImage(pathname, camera, date, mediaType = 'image/jpeg') {
  let operationDefinition = { pathname, commands: [] };
  let thumbnailSpecs = dao.getThumbnailSpecs();

  let requestImageProcess = function requestImageProcess(operationDefinitionBuffer) {
    let responseBody = '';
    let req = http.request({
      port: imageProcessorServicePort,
      host: imageProcessorService,
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        'Content-Length': operationDefinitionBuffer.length
      }
    }, (res) => {

      res.on('data', function (chunk) {
        responseBody += chunk;
      });
      res.on('end', async function() {

        // check for error statuses

        // handle response from the image processor
        if (res.statusCode === 200) {
          message = JSON.stringify(responseBody);
          let img = {
            camera,
            date,
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
        }

        checkQueue();
      });
    });

    req.write(operationDefinitionBuffer);
    req.end();
  };

  for (let thumbSpec of thumbnailSpecs) {
    let command = _.extend({filename: thumbSpec.name}, thumbSpec.spec);
    operationDefinition.commands.push(command);
  }

  requestImageProcess(Buffer.from(JSON.stringify(operationDefinition)));
}

/**
 * @method queueJob 
 * Put a job in the queue for processing.
 * @param {object} metadata image metadata to use to build job.
 * @param {string} metadata.pathname the path of the imagefile to process
 * @param {string} metadata.camera the name of the camera that took the photo
 * @param {Date} metadata.date the date the photo was taken
 * @throws when a job is invalid 
 */
async function queueJob(metadata) {
  // validate job before enqueueing it
  let errors = [];
  if (metadata.pathname) {
    let stat = await fs.stat(metadata.pathname);
    stat.isFile() || errors.push(`File "${metadata.pathname}" does not ` +
      'exist or is not a regular file.');
  } else {
    errors.push('Pathname not specified.');
  }
  if (metadata.camera) {
    // test if the camera exists
    await dao.cameraExists(camera) ||
      errors.push(`Camera "${camera}" unknown`);
  } else {
    errors.push('Camera not specified.');
  }
  if (_.isDate(metadata.date)) {
    errors.push('Image date not specified.');
  }

  if (errors.length > 0) {
    debug('[queueJob] erros in job request:', errors);
    throw { message: 'errors requesting job', details: errors };
  }
  workQueue.push(metadata);
}

/**
 * @method queueStatus
 * Gets the status of a queue. Can be QUEUE_PAUSED or QUEUE_OPEN. Jobs should
 * not be enqueued when the queue is paused. Note `queueJob` will accept jobs
 * regardless of status but it's best to ask first.
 */
function queueStatus() {
  if (workQueue.length >= maxQueue) {
    return QUEUE_PAUSED;
  } else {
    return QUEUE_OPEN;
  }
}

module.exports = {
  queueStatus,
  queueJob,
}
