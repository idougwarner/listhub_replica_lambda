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

const fetchListingData = async (type) => {
  var result = {
    listdataAdded: false,
    listAddError: null,
    datadownloaded: 0,
    remainingToDownload: 0,
  };

  const getSize = (bytes) => {
    var marker = 1024;
    var decimal = 3; // Change as required
    var kiloBytes = marker; // One Kilobyte is 1024 bytes
    var megaBytes = marker * kiloBytes; // One MB is 1024 KB
    var gigaBytes = marker * megaBytes; // One GB is 1024 MB
    var teraBytes = marker * gigaBytes; // One TB is 1024 GB

    // return bytes if less than a KB
    if (bytes < kiloBytes) return { bytes: bytes, type: "Bytes" };
    // return KB if less than a MB
    else if (bytes < megaBytes)
      return { bytes: (bytes / kiloBytes).toFixed(decimal), type: "KB" };
    // return MB if less than a GB
    else if (bytes < gigaBytes)
      return { bytes: (bytes / megaBytes).toFixed(decimal), type: "MB" };
    // return GB if less than a TB
    else if (bytes < teraBytes)
      return { bytes: (bytes / gigaBytes).toFixed(decimal), type: "GB" };
  };

  // Extract new data and store Etag and sequence
  // Run get request to read data to file then read the data to the database
  const getInputStream1 = (rangeValues) => {
    // Get inputStream from replication request
    /*
    return request({
      url: replicationURL,
      headers: {
        Accept: "application/json",
        Authorization: "Bearer " + token,
        Range:
          "bytes=" + rangeValues.startOfRange + "-" + rangeValues.endOfRange,
      },
    });*/

      return axios.get(replicationURL, {
        headers: {
          "Accept": "application/json",
          "Authorization": "Bearer " + token,
          "Range": "bytes=" + rangeValues.startOfRange + "-" + rangeValues.endOfRange,
          'If-Range': type.ETag
          },
      });
  };

  // Get size of file in either B, KB, MB or GB
  const filetoDownloadSize = type.ContentLength;
  const convertedFileDownloadSize = getSize(type.ContentLength);

  let chunkDownloaded = 0;
  let chunks = 0;
  let downloadedSize = 0;
  let chunkSize = 0;
  let step = 0;
  let remainingDownloadSize = 0;
  let startOfRange;
  let endOfRange;

  if (convertedFileDownloadSize.type == "MB") {
    // Check if filesize is between 20MB and 30MB and set chunks to be 5
    if (
      convertedFileDownloadSize.bytes <= 30 &&
      convertedFileDownloadSize >= 20
    ) {
      chunks = 5;
    }
  }

  // If file size is in KB then there is no need to chunk
  else if (convertedFileDownloadSize.type == "KB") {
    chunks = 1;
  }

  // while downloadedfilesize!=contentLength keep downloading in specified chunks
  while (
    downloadedSize != filetoDownloadSize &&
    downloadedSize < filetoDownloadSize
  ) {
    var rangeValues;

    if (chunks == 1) {
      chunkSize = 1024;
    } else if (chunks == 5) {
      chunkSize = parseInt(filetoDownloadSize / chunks) * 1048576;
    }

    /* Download the first part of the data and check if remaining download size is less than the chunk size to 
    determine start and end */
    if (remainingDownloadSize == 0 && step == 0) {
      // Will be true only the first time
      // Set Range
      startOfRange = step === 0 ? 0 + chunkSize * step : 1 + chunkSize * step;
      endOfRange = startOfRange + chunkSize;

      rangeValues = { startOfRange: startOfRange, endOfRange: endOfRange };
    }

    // Check to see if what is remaining is bigger than the chunk size so that we set next range
    else if (
      downloadedSize != 0 &&
      step != 0 &&
      filetoDownloadSize - downloadedSize > chunkSize
    ) {
      // Set Range
      startOfRange = step === 0 ? 0 + chunkSize * step : 1 + chunkSize * step;
      endOfRange = startOfRange + chunkSize;

      rangeValues = {
        startOfRange: 1 + chunkSize * step,
        endOfRange: endOfRange,
      };
    }

    // Check to see if what is remaining is less than chunkSize so as to determine new range limits
    else if (
      downloadedSize != 0 &&
      step != 0 &&
      filetoDownloadSize - downloadedSize < chunkSize
    ) {
      // Set Range
      startOfRange = step === 0 ? 0 + chunkSize * step : 1 + chunkSize * step;
      endOfRange = filetoDownloadSize;

      rangeValues = { startOfRange: startOfRange, endOfRange: endOfRange };
    }

    // Download the data
   
    const response1 = await getInputStream1(rangeValues);
    

    console.log("Response using Axios"+response1);

    const writeStream = async (data) => {

      var myjson = data.toString().split("}{");

        // Create a JSON object array for saving to database
        var mylist = "[" + myjson.join("},{") + "]";

        /* Get the last sequence value and use it to fetch data with Etag to ensure we have fetched everything
          and no data is left
        */
        var lastRecord = JSON.parse(mylist[mylist.length - 1]);

        const listings1 = JSON.parse(mylist);

        // Create All Property Listings at once

        const { dataAdded, error } = await propertyBulkCreate(listings1);

        listdataAdded = dataAdded;
        listError = error;
    };

    console.log("After create a file write stream");

    var listdataAdded;
    var listError;

    // console.log("Status code " + response);
    // console.log("Etag value " + response.headers["ETag"]);

    /*
    inputStream
      .on("response", (response) => {
        console.log("Status code " + response.statusCode);
        console.log("Etag value " + response.headers["ETag"]);
        Etag = response.headers["ETag"];
      })
      .pipe(new JsonLinesTransform())
      .pipe(writeStream())
      .on("finish", async () => {
        console.log("Done downloading Property Listing data!");
      }); // End of Input Stream
    */
  

    if (listdataAdded) {
      // Data was successfully added therefore add step and downloadedSize and proceed to get next chunk in next loop
      step = step + 1;

      chunkDownloaded = step + 1; // We want to know how many chunks are downloaded
      downloadedSize = endOfRange;
      remainingDownloadSize = filetoDownloadSize - downloadedSize;

      result.listAddError = null;
      result.listdataAdded = listdataAdded;
      result.datadownloaded = downloadedSize;
      result.remainingToDownload = remainingDownloadSize;
    } else {
      remainingDownloadSize = filetoDownloadSize - downloadedSize;
      result.remainingToDownload = remainingDownloadSize;
      result.listAddError = listError;

      break;
    }
  } // End While

  return result;
};

// Retrieve new streamed data and store to database
const newListData = async (type) => {
  // Create a time object and store start time we want stream to read data for 7 minutes.
  /* It is possible to finish reading all data in the seven minutes */
  let startTime = new Date();

  const Etag = "";

  if (type.storeType == "new") {
    const {
      listdataAdded,
      listAddError,
      datadownloaded,
      remainingToDownload,
    } = await fetchListingData(type);

    if (listdataAdded) {
      result = {
        listDataAdded: true,
        listAddError: listAddError,
        listDataDownloaded: datadownloaded,
        listRemainingToDownload: remainingToDownload,
      };

      return result;
    } else {
      result = {
        listDataAdded: true,
        listAddError: listAddError,
        listDataDownloaded: datadownloaded,
        listRemainingToDownload: remainingToDownload,
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
        const {
          listdataAdded,
          listAddError,
          datadownloaded,
          remainingToDownload,
        } = await fetchListingData(type);

        if (listdataAdded) {
          result = {
            listDataAdded: listdataAdded,
            listAddError: listAddError,
            listDataDownloaded: datadownloaded,
            listRemainingToDownload: remainingToDownload,
          };

          return result;
        } else {
          result = {
            listDataAdded: listdataAdded,
            listAddError: listAddError,
            listDataDownloaded: datadownloaded,
            listRemainingToDownload: remainingToDownload,
          };

          return result;
        }
      } else {
        result = {
          listDataAdded: false,
          listAddError: "Problem deleting old data",
          listDataDownloaded: 0,
          listRemainingToDownload: type.ContentLength,
        };

        return result;
      }
    } // End of download if Property data already exists

    /*************************************************************************************************************************** */

    // Beginning of download where there is no Property list
    else {
      console.log("Inside else clause No listing data had been saved before ");

      const {
        listdataAdded,
        listAddError,
        datadownloaded,
        remainingToDownload,
      } = await fetchListingData(type);

      if (listdataAdded) {
        result = {
          listDataAdded: listdataAdded,
          listAddError: listAddError,
          listDataDownloaded: datadownloaded,
          listRemainingToDownload: remainingToDownload,
        };

        return result;
      } else {
        result = {
          listDataAdded: listdataAdded,
          listAddError: listAddError,
          listDataDownloaded: datadownloaded,
          listRemainingToDownload: remainingToDownload,
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
  let response = await axios.get(metaURL, {
    headers: {
      Authorization: "Bearer " + token
    },
  });

  if (response) {
    console.log("Last Modified is " + response.data.LastModified);
    console.log("Content Length: " + response.data.ContentLength);
    console.log("Etag Value: " + response.data.ETag);

    // CHECK IF PRODUCT LISTING DATA EXISTS AND IF NOT POPULATE THE LISTINGS TABLE
    const { dataExists } = await propertyDataExists();

    console.log("Data Exists: " + dataExists);

    if (!dataExists) {
      // Call Replicate data to populate new data
      const data = {
        storeType: "new",
        ContentLength: response.data.ContentLength,
        ETag: response.data.ETag
      };

      const { listdataAdded, listerror } = await newListData(data);

      if (listdataAdded) {
        console.log("Product List Data Added");
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
            ETag: response.data.ETag
          };

          const { listdataAdded, listerror } = await newListData(data);

          if (listdataAdded) {
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
