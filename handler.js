"use strict";
const axios = require("axios");
const request = require("request");
const fs = require("fs");
const stream = require("stream");
const https = require("https");

const {
  propertyBulkCreate,
  propertyDataExists,
  propertyDeleteAll,
} = require("./controllers/property.controller");
const {
  metaCreate,
  metaDataExists,
  metaDeleteAll,
  ismetadataNew,
} = require("./controllers/propertymeta.controller");

const { metaURL, replicationURL, token } = require("./config/url");
const { response } = require("express");

class JsonLinesTransform extends stream.Transform {
  _transform(chunk, env, cb) {
    if (!this.chunks) {
      this.chunks = "";
    }

    this.chunks += chunk;

    var lines = this.chunks.split(/\n/);

    this.chunks = lines.pop();

    for (let i = 0; i < lines.length; i++) {
      this.push(lines[i]);
    }

    cb();
  }
}

const getInputStream1 = async (values) => {
  return request({
    url: replicationURL,
    headers: {
      Accept: "application/json",
      Authorization: "Bearer " + token,
      "If-Range": values.ETag,
      Range: "sequence=" + values.sequence + "-",
    },
  });
};

const fetchListingData = async (type) => {
  var result = {
    listdataAdded: false,
    listAddError: null,
  };

  console.log("Inside Test FetchListings");

  const inputStream = await getInputStream1(type);
  const writeStream = fs.createWriteStream("/tmp/propertylisting.json");

  console.log("After create a file write stream");

  inputStream
    .on("data", (response) => {
      console.log("Data: " + response);
    })
    .on("error", (err) => {
      console.log("Error is" + err);
    })
    .pipe(new JsonLinesTransform())
    .pipe(writeStream)
    .on("finish", () => {
      // create a readjson
      const jsonfile = fs.createReadStream("/tmp/propertylisting.json");

      let rawdata = fs.readFileSync("/tmp/propertylisting.json");

      // console.log("RAW Data "+rawdata);

      var myjson = rawdata.toString().split("}{");

      console.log(" Myjson" + myjson);

      // Create a JSON object array
      // [myjson.join('},{')]
      var mylist = "[" + myjson.join("},{") + "]";

      const listings1 = JSON.parse(mylist);
    });

  // Bulk create the data to database

  console.log("After input stream");

  return result;
};

// Retrieve new streamed data and store to database
const newListData = async (type) => {
  // Create a time object and store start time we want stream to read data for 7 minutes.
  /* It is possible to finish reading all data in the seven minutes */
  let startTime = new Date();

  const Etag = "";

  if (type.storeType == "new") {
    const { listdataAdded, listAddError } = await fetchListingData(type);

    if (listdataAdded) {
      result = {
        listDataAdded: true,
        listAddError: listAddError,
      };

      return result;
    } else {
      result = {
        listDataAdded: true,
        listAddError: listAddError,
      };

      return result;
    }
  } // End of new download

  // Fresh listings download
  else if (type.storeType === "newDownload") {
    var result;

    const { dataExists } = await propertyDataExists();

    if (dataExists) {
      // Delete old data and put new data
      const { dataDeleted, error } = await propertyDeleteAll();

      // Old data deleted successfully
      if (dataDeleted) {
        const { listdataAdded, listAddError } = await fetchListingData(type);

        if (listdataAdded) {
          result = {
            listDataAdded: listdataAdded,
            listAddError: listAddError,
          };

          return result;
        } else {
          result = {
            listDataAdded: listdataAdded,
            listAddError: listAddError,
          };

          return result;
        }
      } else {
        result = {
          listDataAdded: false,
          listAddError: "Problem deleting old data",
        };

        return result;
      }
    } // End of download if Property data already exists

    /*************************************************************************************************************************** */

    // Beginning of download where there is no Property list
    else {
      console.log("Inside else clause No listing data had been saved before ");

      const { listdataAdded, listAddError } = await fetchListingData(type);

      if (listdataAdded) {
        result = {
          listDataAdded: listdataAdded,
          listAddError: listAddError,
        };

        return result;
      } else {
        result = {
          listDataAdded: listdataAdded,
          listAddError: listAddError,
        };

        return result;
      }
    } // End of fresh data download
  }
};

const metaStream = async () => {
  // Get inputStream from replication request with range headers
  return axios({
    url: metaURL,
    method: "get",
    headers: {
      Accept: "application/json",
      Authorization: "Bearer " + token,
    },
  });
};

const fetchData = async () => {
  console.log("Inside FetchListings");

  const response = await metaStream();

  if (response) {
    console.log("Last Modified is " + response.data.LastModified);
    console.log("Content Length: " + response.data.ContentLength);
    console.log("Etag Value: " + response.data.ETag);

    var date = new Date();
    date.setSeconds(0);
    date.setMilliseconds(0);

    var key = date.getTime().toString().padEnd(19, 0);

    const metaResponse = await metaStream();

    console.log("MetaData is" + JSON.stringify(metaResponse.data));

    const lastSequence = metaResponse.data.Metadata.lastsequence;
    const sequence = lastSequence - metaResponse.data.Metadata.totallinecount;
    const ETag = metaResponse.data.ETag;

    const values = { ETag: ETag, sequence: sequence };

    console.log("ETag: " + values.ETag + " Sequence: " + values.sequence);

    console.log("KEY is: " + key);

    // CHECK IF PRODUCT LISTING DATA EXISTS AND IF NOT POPULATE THE LISTINGS TABLE
    const { dataExists } = await propertyDataExists();

    console.log("Data Exists: " + dataExists);

    if (!dataExists) {
      // Call Replicate data to populate new data
      const data = {
        storeType: "new",
        ContentLength: response.data.ContentLength,
        ETag: ETag,
        sequence: sequence,
      };

      const { listDataAdded, listAddError } = await newListData(data);

      if (listDataAdded) {
        console.log("Product List Data Added" + listDataAdded);
      } else {
        console.log("Problem adding data");
      }
    } else {
      console.log("Product listing data exists");
    }

    // CHECK WHETHER PROPERTY METADATA EXISTS AND IF NOT CREATE NEW METADATA
    const { metadataExists } = await metaDataExists();

    // If metadata does not exist then store to database
    if (!metadataExists) {
      // Store the new Metadata
      const { metadataAdded } = await metaCreate(response.data);

      // Check if meta has been stored
      if (metadataAdded) {
        console.log("New metadata has been created");
      }
    }

    // NOW THAT WE HAVE POPULATED LISTINGS AND METADATA WE WILL CHECK METADATA TO SEE IF THERE IS NEW UPDATE THAT NEEDS OUR ATTENTION

    // Compare stored meta data and new meta data coming in from Metadata URL to see if we have new listings
    const { newUpdate } = await ismetadataNew(response.data.LastModified);

    if (newUpdate === true) {
      console.log("New listings ready for download: ");

      // If new metadata detected delete old metadata record and save new metadata and call replicationData to download new listings
      const { metadataDeleted, error } = await metaDeleteAll();

      if (metadataDeleted) {
        // Store the new Metadata

        const { metadataAdded, metadata, error } = await metaCreate(
          response.data
        );

        if (metadataAdded) {
          console.log("New Metadata" + JSON.stringify(data));

          const data = {
            storeType: "newDownload",
            ContentLength: response.data.ContentLength,
            ETag: response.data.ETag,
            sequence: key,
          };

          const { listDataAdded, listAddError } = await newListData(data);

          if (listDataAdded) {
            console.log("New Product List Data Added");
          } else {
            console.log("Problem adding data");
          }
        } else {
          console.log("Problem Adding new Meta Data" + error);
        }
      } else {
        console.log("Problem deleting Meta Data " + error);
      }
    } else {
      // Do nothing to existing listings
    }
  }
};

module.exports.fetchListingsData = (event, context) => {
  fetchData();
};

const testInputStreamWithRanges = async (values) => {
  // Get inputStream from replication request with range headers
  return request({
    url: replicationURL,
    headers: {
      Accept: "application/json",
      Authorization: "Bearer " + token,
    },
  });
};

const getData = async () => {
  console.log("Fetch meta Data");

  const metaResponse = await metaStream();

  console.log("MetaData is" + JSON.stringify(metaResponse.data));

  const lastSequence = metaResponse.data.Metadata.lastsequence;
  const sequence = lastSequence - metaResponse.data.Metadata.totallinecount;
  const ETag = metaResponse.data.ETag;

  var values = {
    withRanges: true,
    ETag: ETag,
    startSequence: sequence,
    endSequence: 6000,
  };

  console.log(
    "ETag: " +
      values.ETag +
      " Sequence: " +
      values.startSequence +
      "First take end:" +
      (values.startSequence + 6000)
  );

  console.log("Inside Test FetchListings");

  const inputStream = await testInputStreamWithRanges(values);
  const writeStream = fs.createWriteStream("/tmp/propertylisting.json");

  var date = new Date();
  date.setSeconds(0);
  date.setMilliseconds(0);

  var key = date.getTime().toString().padEnd(19, 0);

  console.log("Sequence Key is " + key);
    
    console.log("Downloading with Ranges");

    console.log("Request Data values: "+JSON.stringify(values))

    // Call stream with Ranges
    const withRanges = await testInputStreamWithRanges(); 

    withRanges
      .on("data", (response) => {
        console.log("Data: " + response);

      })
      .on("error", (err) => {
        console.log("Error is" + err);
      })
      /*
      .pipe(new JsonLinesTransform())
      .pipe(writeStream)
      .on("finish", () => {
        
        const jsonfile = fs.createReadStream("/tmp/propertylisting.json");

        let rawdata = fs.readFileSync("/tmp/propertylisting.json");

        console.log("RAW Data "+rawdata);

        var myjson = jsonfile.toString().split("}{");

        console.log(" Myjson with Ranges" + myjson);

        console.log("After my JSON file reading A");

        // Create a JSON object array
        // [myjson.join('},{')]
        var mylist = "[" + myjson.join("},{") + "]";

        const listings1 = JSON.parse(mylist);
      });*/

};

module.exports.testfetchListingsData = (event, context, callback) => {
  // getData();
  // Call stream with Ranges
    request({
      url: replicationURL,
      headers: {
        Accept: "application/json",
        Authorization: "Bearer " + token,
      },
    })
    .on("data", (response) => {
      console.log("Data: " + response);

    })
    .on("error", (err) => {
      console.log("Error is" + err);
      context.done(null, 'FAILURE');
    })
    .on("finish", () => {

      context.succeed("Sucess")
      /*
      const jsonfile = fs.createReadStream("/tmp/propertylisting.json");

      let rawdata = fs.readFileSync("/tmp/propertylisting.json");

      console.log("RAW Data "+rawdata);

      var myjson = jsonfile.toString().split("}{");

      console.log(" Myjson with Ranges" + myjson);

      console.log("After my JSON file reading A");

      // Create a JSON object array
      // [myjson.join('},{')]
      var mylist = "[" + myjson.join("},{") + "]";

      const listings1 = JSON.parse(mylist);*/
    });
};

module.exports.run = (event, context) => {

  request({
    url: replicationURL,
    headers: {
      Accept: "application/json",
      Authorization: "Bearer " + token,
    },
  })
  .on("data", (response) => {
    console.log("Data: " + response);

  })
  .on("error", (err) => {
    console.log("Error is" + err);
    context.done(null, 'FAILURE');
  })
  .on("finish", () => {

    context.succeed("Sucess")
    /*
    const jsonfile = fs.createReadStream("/tmp/propertylisting.json");

    let rawdata = fs.readFileSync("/tmp/propertylisting.json");

    console.log("RAW Data "+rawdata);

    var myjson = jsonfile.toString().split("}{");

    console.log(" Myjson with Ranges" + myjson);

    console.log("After my JSON file reading A");

    // Create a JSON object array
    // [myjson.join('},{')]
    var mylist = "[" + myjson.join("},{") + "]";

    const listings1 = JSON.parse(mylist);*/
  });

  
  /*const time = new Date();

  const db = {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    name: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
  };

  console.log(
    `Your cron function "${
      context.functionName
    }" ran at ${time} with db ${JSON.stringify(db, null, 2)}`
  );*/
};
