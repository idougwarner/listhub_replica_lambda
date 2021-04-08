'use strict';
const axios = require("axios");
const request = require('request');
const fs = require("fs");
const stream = require("stream");

const properties = require("./controllers/property.controller");
const propertyMeta = require("./controllers/propertymeta.controller");

const { metaURL, replicationURL, token } = require("./config/url");

module.exports.run = async (event, context) => {

  const time = new Date();
  
  const db = {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    name: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD
  };

  console.log(`Your cron function "${context.functionName}" ran at ${time} with db ${JSON.stringify(db, null, 2)}`);
};

module.exports.fetchListingsData = async (event, context) => {

}
