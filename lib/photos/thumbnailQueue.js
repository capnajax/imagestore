'use strict';

const dao = require('../dao');
const debug = require('@capnajax/debug')('imagestore:thumbnailQueue');
const fs = require('fs').promises;
const http = require('http');
const path = require('path');

// tasks yet to be started
let workQueue = [];

const maxThreads = c('max_image_threads');
const imageProcessorService = c('image_processor_service');
const imageProcessorServicePort = c('image_processor_service_port');

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
      processImage(workItem.file, workItem.camera, workItem.data);
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


