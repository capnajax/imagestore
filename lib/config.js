'use strict';
const debug = require('@capnajax/debug')('imagestore:config');
const os = require('os');
const _ = require('lodash');

const DEFAULT_CONFIGS = {

  image_processor_service : 'imageprocessor',
  image_processor_service_port : '80',

  // if there are more than 3 cores, keep two free.
  max_image_threads : Math.max(os.cpus().length - 2, 1),
  // up to 5 items per thread, or 20, whichever is greater.
  max_image_queue : Math.max(os.cpus().length - 2, 4) * 5,

  min_import_interval : 500,
  max_import_interval : 5000,

  'images-path'   : '/images'
};

// Because environment variables are always string, check if these are set
// to String "false" before returning them.
let booleanProperties = new Set();

// Because environment variables are always string, check if these are set
// a numeric value before returning them.
let integerProperties = new Set();
let floatProperties = new Set();

/**
 * @method config
 * Gets the value of a config
 * @param {string} name the name of the config
 * @return {string} the value of the config 
 */
function config(name) {
  debug('[config] called for name', name);
  if (_.has(process.env, name)) {
    debug('[config] found in process.env:', process.env[name]);
    if (booleanProperties.has(name)) {
      debug('[config] converting boolean value');
      if (process.env[name] === "false") {
        return false;
      } else {
        return true;
      }
    }
    try {
      if (integerProperties.has(name)) {
        debug('[config] parsing numeric value');
        return Number.parseInt(process.env[name]);
      }
    } catch(e) {
      // do nothing, go on to next
    }
    try {
      if (floatProperties.has(name)) {
        debug('[config] parsing numeric value');
        return Number.parseFloat(process.env[name]);
      }
    } catch(e) {
      // do nothing, go on to next
    }
    // string
    return process.env[name];
  } else {
    let dcn = DEFAULT_CONFIGS[name];
    debug('[config] got value from DEFAULT_CONFIGS');
    if (_.isObject(dcn)) {
      debug('[config]  --> value is object');
      if (_.has(dcn, '_switch')) {
        debug('[config]  --> object has _switch');
        dcn = config(dcn._switch.name)
          ? dcn[dcn._switch.true]
          : dcn[dcn._switch.false]
      }
    }
    debug('[config] returning value', dcn);
    return dcn;
  }
}

module.exports = config;

