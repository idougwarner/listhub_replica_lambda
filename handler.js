"use strict";
const axios = require("axios");
const request = require("request");
const fs = require("fs");
const stream = require("stream");
const https = require("https");
const JSONStream = require('JSONStream');
const es = require('event-stream');
var pg = require('pg');

var dbUrl = 'postgres://postgres:postgres@listhub-dev.crstoxoylybt.us-west-2.rds.amazonaws.com:5432/listhubdev';

const { Pool, Client } = require("pg");
const copyFrom = require('pg-copy-streams').from

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT
});

const { syncDB } = require("./models");

const {
  list_a_Create,
  list_a_BulkCreate,
  list_a_BulkList,
  list_a_DataExists,
  list_a_DeleteAll,
} = require("./controllers/listhub_listings_a.controller");

const {
  list_b_Create,
  list_b_BulkCreate,
  list_b_BulkList,
  list_b_DataExists,
  list_b_DeleteAll,
} = require("./controllers/listhub_listings_b.controller");

const {
  listCreate,
  listBulkCreate,
  listBulkList,
  listDataExists,
  listDeleteAll,
} = require("./controllers/listings_update_reference.controller");

const {
  metaCreate,
  metaDataExists,
  metaDeleteAll,
  ismetadataNew,
} = require("./controllers/listings_meta.controller");

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

// Fetch MetaData
const getMetaDataStream = async () => {

  // Get inputStream from replication request with range headers
  return axios({
    url: metaURL,
    method: "get",
    headers: {
      Accept: "application/json",
      Authorization: "Bearer " + token,
    },
  })

};

// Get the List Data and save to Database
const getListingStream = async (values) => {

  var listings = ""
  var listArray = []

  const writeStream = fs.createWriteStream("/tmp/propertylisting.json");

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

      return new Promise((resolve, reject) => {

      // STREAMING WITH JSON STREAM
      var startTime, endTime

      startTime=new Date()
      
      console.log("Start Time: "+startTime)

      pool.connect((err, client, done) => {

      var count;

      count = 0

      var time = new Date()

      var targetTable="listhub_listings_as"

      /*stream
        .pipe(new JsonLinesTransform())
        .pipe(writeStream);
      
      stream.on("complete",()=> {

        console.log("Finished copying to csv file")

      })
      stream.on("error",(err)=>{
        console.log("Error is: "+err)
      })*/

      var stream1 = client.query(copyFrom(`COPY ${targetTable} FROM STDIN`))
     // var fileStream = fs.createReadStream(inputFile)

      stream.on('error', (error) =>{
          console.log(`Error in reading file: ${error}`)
      })
      stream1.on('error', (error) => {
          console.log(`Error in copy command: ${error}`)
      })
      stream1.on('end', () => {
          console.log(`Completed loading data into ${targetTable}`)
          client.end()
      })

      stream.pipe(stream1);
    
      /*stream
      .pipe(JSONStream.parse())
      .pipe(es.mapSync((data) => {

          //listArray.push(data)
          listings=listings+JSON.stringify(data)

          // Convert this data to a string, now we need to write it to a csv
         // console.log(JSON.stringify(data))
            
      }))*/

      stream.on("complete", () => {

        // COPY table FROM '/tmp/table.csv' DELIMITER ',';

        /*client.query(
          'INSERT INTO "listhub_listings_as" ("sequence","Property", "createdAt", "updatedAt") VALUES ($1,$2,$3,$4) RETURNING id', 
          [data.sequence, data.Property, time, time], 
          function(err, result) {
              if (err) {
                  console.log(err);
              } else {
                  console.log('row inserted with id: ' + result.rows[0].id);
              }

              count++;

              console.log('count = ' + count);
              
              if (count == listArray.length) {
                  console.log('Client will end now!!!');
                  
              }
      })*/

        /*
        client.query(
          'INSERT INTO "listhub_listings_as" ("sequence","Property", "createdAt", "updatedAt") VALUES ($1,$2,$3,$4) RETURNING id', 
          [data.sequence, data.Property, time, time], 
          function(err, result) {
              if (err) {
                  console.log(err);
              } else {
                  console.log('row inserted with id: ' + result.rows[0].id);
              }

              count++;

              console.log('count = ' + count);
              
              if (count == listArray.length) {
                  console.log('Client will end now!!!');
                  
              }
      });// End of Client Query  
        
        console.log("Completed reading of data: "+listArray.length)

       // client.end();

        var time=new Date()*/
        
      }) // End of Stream Complete 

      /*
      stream
      .on("data", (data) => {

        // Append our data to the array as we read it
       
        console.log("Chunk is:"+data)
                
        listings = listings+data

        //console.log(mylist.toString())

        // Bulk Write to database
        //writeStream.write(myjson)
    
      })
      .on("complete", () => {

        //console.log(listings)

        var myjson = listings.split(/\n/);

        var lastEmpty= myjson.pop()
        
        var mylist = "[" + myjson.toString() + "]";

        const listings1 = JSON.parse(mylist);

        //console.log(mylist.toString())

        // Bulk Write to database
        //writeStream.write(myjson)

        console.log("Listing Data...\n"+listings)
        
        propertyBulkCreate(listings1).then((response)=>{
          console.log("Data Added To DB"+JSON.stringify(response))
        }).catch((err)=>{
          console.log("Error from DB"+err)
        })
          
        // If this works we will parse the entire array and bulkSave to database and resolve to return to our caller
        console.log("Finished reading data\n")
      
        console.log('Downloaded data....\nStart Sequence: '+values.startSequence+" End Sequence: "+values.endSequence)

        downloaded = true; 
        listerror = null; //startSequence: values.startSequence, endSequence: values.endSequence }

        resolve({downloaded: downloaded, error:listerror})

      })*/

    }); // End of Pool Connect

    })// End Promise

};

// Sync entire data while using ranges and save to database
const saveNewListData = async () => {

  var metaResponse;
  var lastSequence = 0;
  var startSequence = 0 ;
  var ETag;
  var count = 1;
  var endSequence = 0;
  var secondStart = 0 ;
  var listDataAdded = "";
  var listAddError = "";
  var start = "";

  try {

    metaResponse = await getMetaDataStream();

    console.log("Status code is: "+metaResponse);

    lastSequence = metaResponse.data.Metadata.lastsequence;
    startSequence = lastSequence - metaResponse.data.Metadata.totallinecount;
    ETag = metaResponse.data.ETag;

    console.log("MetaData is" + JSON.stringify(metaResponse.data)+"Status code is: "+metaResponse.statusCode);

  }

  catch(err) {

    listDataAdded = false; 
    listAddError = err;

    console.log("Error is: "+err)

  }

  const totallinecount = metaResponse.data.Metadata.totallinecount;
  
  var chunkSize = parseInt(totallinecount/5);
  var secondChunk = chunkSize+1;

  var values;

  // console.log("Chunk Size:"+chunkSize+"Total Line Count: "+totallinecount+"StartSequence "+startSequence+"End Sequence"+lastSequence)

  // I want to divide the calls to 2, I want to halve the listings. The first will call will be startSting sequence+what is halved
  // Next call will be the previous call end sequencccccce
  while (
    count <= 15
  ) {

      // First Chunk
      if(count==1) {

        endSequence=startSequence+chunkSize;

        console.log("Step "+count+' \nStart Sequence: '+startSequence+" End Sequence: "+endSequence);

        values = {
          ETag: ETag,
          startSequence: startSequence,
          endSequence: endSequence,
        };
      }

      else if(count==15) {

        startSequence = endSequence + 1
        endSequence = startSequence + chunkSize

        console.log("Step "+count+' \nStart Sequence: '+startSequence+" End Sequence: "+endSequence);
        
        console.log("Second Start is: "+startSequence)

        values = {
          ETag: ETag,
          startSequence: startSequence,
          endSequence: "",
        };

      }

      // Third Chunk
      else {

        startSequence = endSequence + 1
        endSequence = startSequence + chunkSize

        console.log("Step "+count+' \nStart Sequence: '+startSequence+" End Sequence: "+endSequence);

        values = {
          ETag: ETag,
          startSequence: startSequence,
          endSequence: endSequence,
        };

      }

      const downloadResponse = await getListingStream(values)

      if(downloadResponse.downloaded){

        console.log("Data was downloaded")
        listDataAdded = true
        listAddError = false

        //console.log("ChunkSize+1: "+(chunkSize+1))
        count=count+1

      }
      else {
        console.log("Error Downloading Data")
        listDataAdded = false
        listAddError = true 
      }
 
    } // End WHILE LOOP TO FETCH DATA

    // Return our promise here
    return ({ listDataAdded: listDataAdded, listAddError:listAddError })


}; // End of saveNewListData

const fetchData = async () => {

  // Sync Database first
  await syncDB();

  console.log("Inside FetchListings");

  try {

    const response =  await getMetaDataStream();

    if (response) {
      console.log("Last Modified is " + response.data.LastModified);
      console.log("Content Length: " + response.data.ContentLength);
      console.log("Etag Value: " + response.data.ETag);
  
      var date = new Date();
      date.setSeconds(0);
      date.setMilliseconds(0);
  
      var key = date.getTime().toString().padEnd(19, 0);
  
      const metaResponse = await getMetaDataStream();
  
      console.log("MetaData is" + JSON.stringify(metaResponse.data));
  
      const lastSequence = metaResponse.data.Metadata.lastsequence;
      const sequence = lastSequence - metaResponse.data.Metadata.totallinecount;
      const ETag = metaResponse.data.ETag;
  
      const values = { ETag: ETag, sequence: sequence };
  
      console.log("ETag: " + values.ETag + " Sequence: " + values.sequence);
  
      console.log("KEY is: " + key);
  
      // Check if Listhub listings exist in both listings a and listings b
      const { data_a_Exists } = await list_a_DataExists();
      const { data_b_Exists } = await list_b_DataExists();
  
      console.log("Does Data Exist in Listings A: " + data_a_Exists + "Does Data Exist in Listings B: " + data_b_Exists);
  
      if (!data_a_Exists) {
  
        // Listings A data does not exist therefore populate table a fresh with getData
        const data = {
          storeType: "new",
          ContentLength: response.data.ContentLength,
          ETag: ETag,
          sequence: sequence,
        };
  
        const { listDataAdded, listAddError } = await saveNewListData();
  
        if (listDataAdded) {
          // We should copy data in list_a to list_b
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

          // CHECK   
          const { metadataAdded, metadata, error } = await metaCreate(
            response.data
          );
  
          if (metadataAdded) {
            console.log("New Metadata" + JSON.stringify(data));
  
            const { listDataAdded, listAddError } = await saveNewListData();
  
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
  
        console.log("New Update: "+newUpdate)
        // Do nothing to existing listings
      }
    }

  }// End of try Block

  catch(err) {

    //console.log(err)
    
    if(err.response.status==401) {
      console.log("You are not authorized to access Listhub API Obtain acces from admin")
    }

  }

};

module.exports.fetchListingsData = (event, context) => {
  fetchData();
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

      const listings1 = JSON.parse(mylist);
      */
    });
};

module.exports.run = (event, context) => {  
 
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