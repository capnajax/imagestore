
const c = require('../lib/config');
const catalog = require('../lib/catalog');
const express = require('express');
const router = express.Router();
const _ = require('lodash');

const ACCEPTABLE_IMAGES = {
  'image/jpeg': 'jpg'
};

/* POST image */
router.post('/:version/:camera', async function(req, res) {

  console.log("got request");

  let contentType = req.get('Content-Type');

  if (!_.has(ACCEPTABLE_IMAGES, contentType)) {

    console.log("contentType unacceptable", contentType);

    res.sendStatus(415); // unsupported media type

  } else {

    try {

      console.log("storing image");

      catalog.storeImage({
        version: req.params.version,
        camera: req.params.camera,
        format: ACCEPTABLE_IMAGES[contentType]
      }, req.body);

      res.sendStatus(204);

    } catch(e) {
      console.log('Request failed:', e);
      res.sendStatus(500);    
    }
  }
});

module.exports = router;
