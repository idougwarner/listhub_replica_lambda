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

// Get the List Data and save to Database
const getListingStream = async (values) => {

  var listings = ""
  var listingsArray = []

  return new Promise((resolve, reject) => {

      // Get inputStream from replication request with range headers
     var stream = request({
        url: replicationURL,
        headers: {
          Accept: "application/json",
          Authorization: "Bearer " + token,
          "If-Range": values.ETag,
          Range: "sequence=" + values.startSequence + "-"+values.endSequence
        }
      })

      stream
      .on("data", (data) => {

        // Append our data to the array as we read it
        //console.log("Chunk is:"+data)        
        listings = listings+data
    
      })
      .on("complete", () => {

        /*myVar.split('\n').map(JSON.parse);
        console.log(myArray);*/

        //console.log(listings)

        var myjson = listings.split(/\n/);

        var lastEmpty= myjson.pop()
        
        var mylist = "[" + myjson.toString() + "]";

        const listings1 = JSON.parse(mylist);
        // SAVE TO DATABASE

        //console.log(mylist.toString())

        // Bulk Write to database
        //writeStream.write(myjson)

        //console.log("Listing Data...\n"+listings)
        
        // If this works we will parse the entire array and bulkSave to database and resolve to return to our caller
        console.log("Finished reading data\n")
       
        console.log('Downloaded data....\nStart Sequence: '+values.startSequence+" End Sequence: "+values.endSequence)

        resolve({ downloaded: true, error:null, startSequence: values.startSequence, endSequence: values.endSequence })

      })
      .on("error", (err) => {
        
        console.log("Error: "+err)

        resolve({downloaded:false, error:err});

      })
      .on("response", (response) => {

        console.log("Status Code:"+response.statusCode+" Aborted: "+response.aborted+" ")

      })

    })// End of Promise
};

// Sync entire data while using ranges and save to database
const saveNewListData = async () => {

  var metaResponse;
  var lastSequence = 0;
  var startSequence = 0 ;
  var ETag;
  var count = 0;
  var endSequence = 0;
  var secondStart = 0 ;

  try {

    metaResponse = await getMetaDataStream();

    console.log("Status code is: "+metaResponse);

    lastSequence = metaResponse.data.Metadata.lastsequence;
    startSequence = lastSequence - metaResponse.data.Metadata.totallinecount;
    ETag = metaResponse.data.ETag;

    console.log("MetaData is" + JSON.stringify(metaResponse.data)+"Status code is: "+metaResponse.statusCode);

  }

  catch(err) {

    console.log("Error is: "+err)

  }

  const totallinecount = metaResponse.data.Metadata.totallinecount;
  
  var chunkSize = parseInt(totallinecount/2);
  var secondChunk = parseInt(totallinecount/2)+1;

  var values;

  console.log("Chunk Size:"+chunkSize+"Total Line Count: "+totallinecount+"StartSequence "+startSequence+"End Sequence"+lastSequence)

  // I want to divide the calls to 2, I want to halve the listings. The first will call will be startSting sequence+what is halved
  // Next call will be the previous call end sequencccccce
  while (
    count < 2
  ) {

      if(count==0) {

        endSequence=startSequence+chunkSize;

        console.log("Step "+count+' \nStart Sequence: '+startSequence+" End Sequence: "+endSequence);

        values = {
          ETag: ETag,
          startSequence: startSequence,
          endSequence: endSequence,
        };
      }

      else if(count==1) {

        console.log("Second Chunk:"+secondChunk)

        secondStart=startSequence+secondChunk
        console.log("Step "+count+' \nStart Sequence: '+secondStart+" End Sequence: \" \"");

        values = {
          ETag: ETag,
          startSequence: secondStart,
          endSequence: "",
        };

      }

      const downloadResponse = await getListingStream(values)

      if(downloadResponse.downloaded){
        console.log("Data was downloaded")

        //console.log("ChunkSize+1: "+(chunkSize+1))
        count=count+1

      }
      else {
        console.log("Error Downloading Data")
      }
 
    } // End WHILE LOOP TO FETCH DATA
    
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

      // Property data does not exist therefore populate table a fresh with getData
      const data = {
        storeType: "new",
        ContentLength: response.data.ContentLength,
        ETag: ETag,
        sequence: sequence,
      };

      const { listDataAdded, listAddError } = await saveNewListData();

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

  setInterval(() => { 
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
    }, 500)

  

  console.log("After fetch function")

  
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