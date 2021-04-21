"use strict";
const axios = require("axios");
const request = require("request");
const fs = require("fs");
const stream = require("stream");
const https = require("https");
const JSONStream = require('JSONStream');
const es = require('event-stream');
var pg = require('pg');
const AWS = require('aws-sdk');
const lambda = new AWS.Lambda({
  region: "us-west-2"
});

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
  table_to_save_listings
} = require("./controllers/listings_update_reference.controller");

const {
  metaCreate,
  metaDeleteAll,
  metaDataExists,
  ismetadataNew,
  is_meta_data_new
} = require("./controllers/listings_meta.controller");

const { metaURL, replicationURL, token } = require("./config/url");
const { response } = require("express");
const tbl_listings_meta="listings_meta";

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

 const readWriteListingData = async (values) => {

  // Get inputStream from replication request with range headers
  var stream1 = request({
    url: replicationURL,
    headers: {
      Accept: "application/json",
      Authorization: "Bearer " + token,
      "If-Range": values.ETag,
      Range: "sequence=" + values.startSequence + "-"+values.endSequence
    }
  })

  const uploadStream = ({ Bucket, Key }) => {
    const s3 = new AWS.S3();
    const pass = new stream.PassThrough();
    return {
      writeStream: pass,
      promise: s3.upload({ Bucket, Key, Body: pass }).promise(),
    };
  }

  const params = {
    Bucket: 'listhubdev',
    key: 'propertylisting.json'
   };
  
  const { writeStream, promise } = uploadStream(params);

  return new Promise((resolve, reject) => {

    const pipeline = stream1.pipe(writeStream)

    pipeline.on('close', () => {
      console.log('upload successful');
      resolve({savedData:true})
    });
    pipeline.on('error', (err) => {
      console.log('upload failed', err.message)
      resolve({savedData:false})
    });
    
  })
}

const set_meta_table = async (meta_table) => {

  try {
    const client = await pool.connect()
  
  //id, last_modifed, content_length, etag, content_type
  // Get list_a_time_modifed
  client.query(`DROP TABLE IF EXISTS ${meta_table} CASCADE`, (err, result) => {
    if (err) {
        console.log(err);
        return ({ table_created:false })
    } else {

        console.log(`Table ${meta_table} deleted successfully`);

        client.query(`CREATE TABLE IF NOT EXISTS ${meta_table}(id SERIAL PRIMARY KEY, last_modifed TEXT, content_length BIGINT, etag TEXT UNIQUE, content_type TEXT)`, (err, result) => {

            if (err) {
                console.log(err);
            } else {

              console.log(`Table ${meta_table} created successfully"`);

              return ({ table_created:true })   
            }
        })// End of Create Table
      }
    })
  } catch(err) {

    console.log("Create table listings error"+err)
   // return ({updated:false, data: result.rows[0], error:err}) 

  }
}

const set_listings_table = async (table_to_set) => {

  try {
    const client = await pool.connect()

    return new Promise((resolve, reject) => {
      // Get list_a_time_modifed
      client.query(`DROP TABLE IF EXISTS ${table_to_set} CASCADE`, (err, res) => {
        if (err) {
            console.log(err);
            resolve({ table_created:false })
        } else {

            console.log(`Table ${table_to_set} deleted successfully`);

            client.query(`CREATE TABLE IF NOT EXISTS ${table_to_set}(sequence TEXT UNIQUE, property JSON)`, (err, res) => {

                if (err) {
                    console.log(err);
                } else {

                  console.log(`Table ${table_to_set} created successfully"`);

                  resolve({ table_created:true })
                }
            })// End of Create Table
          }
        })

    })// End of Promise
        
    

  } catch(err) {

    console.log("Create table listings error"+err)
    return ({table_created:false, data: null, error:err})

  }
  
}

const create_new_meta_data = async (data) => {

  console.log("Inside create new metadata")

  try {
      const client = await pool.connect()

      return new Promise((resolve, reject) => { 
        client.query(`INSERT INTO ${tbl_listings_meta} (id, last_modifed, content_length, etag, content_type) VALUES (DEFAULT, $1,$2,$3,$4) RETURNING id`, 
          [data.LastModified, data.ContentLength, data.ETag, data.ContentType], (err, res) => {
              if (err) {
                  console.log(err);
                  resolve({ metadataAdded: false, error: "Could Not add Data", metadata: null})
          
              } else {
                  console.log('row inserted with id: ' + res.rows[0].id);
                  
                  resolve({ metadataAdded: true, metadata: data, error: null })

              }
        })
    })// End of Promise
  }
  catch(err) {

    console.log("Error "+err) 

    return ({
      metadataAdded: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Could not create the PropertyMeta.",
      error: err,
      metadata: null,
    })    
  }
}

const meta_data_exist = async () => {
  const tbl_listings_meta="listings_meta"; 

  try {  

    const client = await pool.connect();

    return new Promise((resolve, reject) => {
      client.query(`SELECT * from ${tbl_listings_meta}`, (err, res) => {
        
        if(err) {
          console.log("Check error"+err)

          resolve({ dataExists: false, metadata: null, error: err, statusCode: null,  headers: null,
            body: "" })
        }

        console.log("Rows: "+JSON.stringify(res))

        if(res.rowCount!=0) {

          console.log("Meta Data does exist")

          resolve ({ dataExists: true, metadata: res.rows, error: null, statusCode: 200,  headers: null,
            body: "Successfully created data" })
            
        } else {

          console.log("Meta Data does not exist")
    
          resolve ({ dataExists: false, metadata: null, error: null, statusCode: 500,  headers: null,
            body: "Successfully created data" })
        }
        
      });
    })
  
  }
  catch(err) {

      console.log("Error in meta"+err)

      return ({ dataExists: false, metadata: null, error: err, statusCode: 500,  headers: { "Content-Type": "text/plain" },
        body: "Problem finding PropertyMeta Info." })
  }
};

/**
 *  ListHubMonitor
 *  Invokes every 1 hour and detects any update. 
 */
module.exports.listhubMonitor = async (event, context) => {

  try {

    var table_a = "listhub_listings_a"
    var table_b = "listhub_listings_b"
    var meta_table = "listings_meta"

    const {table_created} = await set_listings_table(table_a)
    console.log(table_a + " created "+table_created)

    await set_listings_table(table_b)
    await set_meta_table(meta_table)
    
    // Get meta_data info
    const response =  await getMetaDataStream();
    
    if (response) {
      
      // Check whether there is new meta_data
      const {dataExists} = await meta_data_exist();

      console.log("Data: dataExists "+dataExists)  
      // Store meta_data if none exists
      if (!dataExists) {
        
        // Store the new Metadata
        const { metadataAdded } = await create_new_meta_data(response.data);

        console.log("Meta Data Added: "+metadataAdded)
  
        // Check if meta_data has been stored for the first time
        if (metadataAdded) {
          
          console.log("New metadata has been created");

          var lastSequence = response.data.Metadata.lastsequence;
          var startSequence = lastSequence - response.data.Metadata.totallinecount;
          var endSequence=0
          var ETag = response.data.ETag;
          
          var i, range;
          range = 15

          const totallinecount = response.data.Metadata.totallinecount;
  
          var chunkSize = parseInt(totallinecount/range);

          var values;
          var ranges = []

          for(i=1; i<=range; i++) {

            if(i==1) {

              startSequence = (lastSequence - response.data.Metadata.totallinecount);
              endSequence = (startSequence+chunkSize);
              
              values = {
                ETag: ETag,
                startSequence: startSequence,
                endSequence: endSequence,
              };

              console.log("Start Sequence:"+ startSequence + "End Sequence:" + endSequence)
              ranges.push(values)
            }

            else if(i==range) {

              startSequence = (endSequence + 1)
              endSequence = ""

              values = {
                ETag: ETag,
                startSequence: startSequence,
                endSequence: endSequence,
              };

              console.log("Start Sequence:"+ startSequence + "End Sequence:" + endSequence)
              ranges.push(values)

            }

            // All chunks in between 1 and the last
            else {

              startSequence = (endSequence + 1)
              endSequence = (startSequence + chunkSize)

              values = {
                ETag: ETag,
                startSequence: startSequence,
                endSequence: endSequence,
              };

              console.log("Start Sequence:"+ startSequence + "End Sequence:" + endSequence)
              ranges.push(values)
            }

          }
          
          // Download new listings by calling StreamExecutor with table_name and ranges
          // We shall download to two tables at the same time
          for(var index=1; index<ranges.length; index++) {

              var range = ranges[index]

              const params1 = {
                FunctionName: "streamExecutor",
                InvocationType: "RequestResponse",
                Payload: JSON.stringify({ "range": range, "table_name": table_a })
              };
          
               lambda.invoke(params1, function(error, data) {
                if (error) {
                  console.error("Error in call table_a: "+JSON.stringify(error));
                  return new Error(`Error printing messages: ${JSON.stringify(error)}`);
                } else if (data) {
                  console.log(data);
                }
              });

              const params2 = {
                FunctionName: "streamExecutor",
                InvocationType: "RequestResponse",
                Payload: JSON.stringify({ "range": range, "table_name": table_b })
              };
          
              lambda.invoke(params2, function(error, data) {
                if (error) {
                  console.error("Error in call table_b"+JSON.stringify(error));
                  return new Error(`Error printing messages: ${JSON.stringify(error)}`);
                } else if (data) {
                  console.log(data);
                }
              });
                                         
          }
        }
        else {

          console.log("Problem creating meta Data Please try later")

        }
      }// End If metadataExists
      else {

        // Compare stored meta_data and new meta_data coming in from listhub to see if we have new listings
        const { newUpdate } = await is_meta_data_new(response.data.LastModified);

        if(newUpdate) {
          // Delete old meta and Download new Meta Data

          const { metadataDeleted, error } = await metaDeleteAll();

          if(metadataDeleted) {

            const { metadataAdded } = await create_new_meta_data(response.data);

            // Check which table to save new data
            const {table_to_save} = await table_to_save_listings()

            // Call StreamExecutor with table_to_save and ranges
            var lastSequence = response.data.Metadata.lastsequence;
            var startSequence = lastSequence - response.data.Metadata.totallinecount;
            var endSequence=0
            var ETag = response.data.ETag;
            
            var i, range;
            range = 15

            const totallinecount = response.data.Metadata.totallinecount;
    
            var chunkSize = parseInt(totallinecount/range);

            var values;
            var ranges = []

            for(i=1; i<=range; i++) {

              if(i==1) {

                startSequence = lastSequence - response.data.Metadata.totallinecount;
                endSequence = startSequence+chunkSize;
                
                values = {
                  ETag: ETag,
                  startSequence: startSequence,
                  endSequence: endSequence,
                };

                ranges.push(values)

              }

              else if(i==range) {

                startSequence = endSequence + 1
                endSequence = startSequence + chunkSize

                values = {
                  ETag: ETag,
                  startSequence: startSequence,
                  endSequence: "",
                };

                ranges.push(values)

              }

              // All chunks in between 1 and the last
              else {

                startSequence = endSequence + 1
                endSequence = startSequence + chunkSize

                values = {
                  ETag: ETag,
                  startSequence: startSequence,
                  endSequence: endSequence,
                };

                ranges.push(values)
              }

            }
            
            // Download new listings by calling StreamExecutor with table_name and ranges
            // We shall download to two tables at the same time
            for(var index=1; index<ranges.length; index++) {

              var range = ranges[index]

              // This will call the lambdas asynchronously
              // Get response of each save so that we update live status of table

              console.log("Stringify options"+JSON.stringify({range, table_to_save}))
              
              const params = {
                FunctionName: "listhub-replica-dev-streamExecutor",
                InvocationType: "EventResponse",
                Payload: JSON.stringify("Mark")
              };
          
              /*lambda.invoke(params, (error, data) => {
                if (error) {

                  console.error("Payload Error"+JSON.stringify(error));
                  return new Error(`Error printing messages: ${JSON.stringify(error)}`);

                } else if (data) {
                  console.log(data);
                }
              });*/
                                          
            }

          }
                    
        } 

      }

    }

  }
  catch(err) {
    console.log("Error: "+err)
  }
    
}

/**
 * StreamExecutor
 * Streams the range and adds all listings in that range to the database 
 */
module.exports.streamExecutor = async (event, context, callback) => {

  console.log("From Monitor "+event)
  
  var ETag = event.range.values.ETag
  var startSequence = event.range.startSequence
  var endSequence = event.range.endSequence
  var table_name = event.range.table_name
  var listings = ""
  var listArray = []

      // Get inputStream from replication request with range headers
     var stream = request({
        url: replicationURL,
        headers: {
          Accept: "application/json",
          Authorization: "Bearer " + token,
          "If-Range": ETag,
          Range: "sequence=" + startSequence + "-" + endSequence
        }
      })

      // STREAMING WITH JSON STREAM
      // var startTime, endTime

      // startTime=new Date()
      
      console.log("Start Time: "+startTime)

      stream
      .pipe(JSONStream.parse())
      .pipe(es.mapSync((data) => {

          listArray.push(data)

      }))

      stream.on("complete", () => {
        
        console.log("Completed reading of data: "+listArray.length)

        var time=new Date()

        pool.connect((err, client, done) => {
              
          var i = 0, count = 0;

          for (i = 0; i < listArray.length; i++) {

              client.query(`INSERT INTO ${table_name} (sequence,Property) VALUES ($1,$2) RETURNING id`, 
                  [listArray[i].sequence, listArray[i].Property], (err, result) => {
                      if (err) {
                          console.log(err);
                      } else {
                          console.log('row inserted with id: ' + result.rows[0].id);
                      }
      
                      count++;
                      console.log('count = ' + count);
                      if (count == listArray.length) {
                          console.log('Lists added successfully Connections will end now!!!');

                          const response = {
                            statusCode: 200,
                            body: JSON.stringify({
                              message: 'Lists Added successfully'
                            })
                          };

                          client.end();
                          callback(null, response);
                      }
                });        
          }
      });
        
      })

}


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