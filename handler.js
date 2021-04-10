"use strict";
const axios = require("axios");
const request = require("request");
const fs = require("fs");
const stream = require("stream");
const util = require("util");

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

  /*
      "If-Range": values.ETag,
      Range: "sequence="+values.sequence + "-"
  */

  const writeStream = fs.createWriteStream("/tmp/propertylisting.json");

  const inputStream = request({
    url: replicationURL,
    headers: {
      Accept: "application/json",
      Authorization: "Bearer " + token,
    },
  });

  
  let response = axios({
    method: "get",
    url: replicationURL,
    headers: {
      Accept: "application/json",
      Authorization: "Bearer " + token,
    },
    responseType: "stream",
  });

  // response.data.pipe(new JsonLinesTransform())
  
    response.data
          .on('data', chunk => {
            downloadedSize += chunk.length;

            console.log('Downloading ', downloadedSize)
          })
          .pipe(new JsonLinesTransform())
          .pipe(writeStream)

          /*return new Promise((resolve, reject) => {

            writeStream.on('end', resolve({writtenData:true}))
            writeStream.on('error', reject({writtenData:false}))

          })

  inputStream
        .pipe(new JsonLinesTransform())
        .pipe(writeStream)
        .on("finish", () => {
        return new Promise((resolve, reject) => {
          resolve({ writtenData: true });

          //response.on('end', resolve({writtenData:true}))
          //writeStream.on('error', reject({writtenData:false}))
        });
  });*/
};

const fetchListingData = async (type) => {
  var result = {
    listdataAdded: false,
    listAddError: null,
  };

  const response1 = await getInputStream1(type);

  console.log("Response using Axios " + JSON.stringify(response1));

  let rawdata = fs.readFileSync("/tmp/propertylisting.json");

  console.log(rawdata);

  /*
    response1
        .on('response', (response) => {
          console.log("Status code "+response.statusCode);
          console.log("ETag value "+response.headers['ETag']);
                        
        })
        .on('data', ()=> {
          console.log("Inside Data get")
        })
        .pipe(new JsonLinesTransform())
        .pipe(writeStream)
        .on('finish', async () => {

          console.log("Done creating a file write stream");

          let rawdata = fs.readFileSync('/tmp/propertylisting.json');
                            
          // var myjson = rawdata.toString().split('}{'); 
          
          var myjson = rawdata.toString().split('}{');

          // Create a JSON object array for saving to database
          var mylist = '[' + myjson.join('},{') + ']';

          const listings1 = JSON.parse(mylist);
          
          const { dataAdded, error} = await propertyBulkCreate(listings1)
                      
            if(dataAdded) {
              result.listdataAdded=true
            } else {
              result.listAddError = true
              result.listdataAdded = false
            }
        })*/

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

module.exports.run = async (event, context) => {
  const time = new Date();

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
  );
};

module.exports.fetchListingsData = async (event, context) => {
  console.log("Inside FetchListings");

  // Call Metadata URL to get necessary data
  let response = await axios({
    method: "get",
    url: metaURL,
    headers: {
      Authorization: "Bearer " + token,
    },
  });

  if (response) {
    console.log("Last Modified is " + response.data.LastModified);
    console.log("Content Length: " + response.data.ContentLength);
    console.log("Etag Value: " + response.data.ETag);

    var date = new Date();
    date.setSeconds(0);
    date.setMilliseconds(0);

    var key = date.getTime().toString().padEnd(19, 0);

    console.log("KEY is: " + key);

    // CHECK IF PRODUCT LISTING DATA EXISTS AND IF NOT POPULATE THE LISTINGS TABLE
    const { dataExists } = await propertyDataExists();

    console.log("Data Exists: " + dataExists);

    if (!dataExists) {
      // Call Replicate data to populate new data
      const data = {
        storeType: "new",
        ContentLength: response.data.ContentLength,
        ETag: response.data.ETag,
        sequence: key,
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
            sequence: key
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

module.exports.testfetchListingsData = async (event, context) => {
  
  console.log("Inside Test FetchListings");

  const writeStream = fs.createWriteStream("/tmp/propertylisting.json");

  const inputStream = request({
    url: replicationURL,
    headers: {
      Accept: "application/json",
      Authorization: "Bearer " + token,
    },
  });
  
   const readData = async () => {
      
    return axios({
      method: 'get',
      url: replicationURL,
      headers: {
        Accept: "application/json",
        Authorization: "Bearer " + token,
      },
      responseType: 'stream'
    }).then(response => {
  
      //ensure that the user can call `then()` only when the file has
      //been downloaded entirely.
  
      return new Promise((resolve, reject) => {

        console.log("Response using Axios " + response);

        response.data.pipe(writeStream);

        let error = null;
        
        writer.on('error', err => {
          error = err;
          writer.close();
          reject(err);
        });

        writer.on('close', () => {
          if (!error) {
            resolve(true);
          }
          // no need to call the reject here, as it will have been called in the
          //'error' stream;
        });

      });

    });

  }

  await readData()

  let rawdata = fs.readFileSync("/tmp/propertylisting.json");

  console.log(rawdata);

};
