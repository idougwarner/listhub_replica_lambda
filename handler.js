"use strict";
const axios = require("axios");
const request = require("request");
const fs = require("fs");
const stream = require("stream");
const https = require("https");
const JSONStream = require("JSONStream");
const es = require("event-stream");
var pg = require("pg");
const AWS = require("aws-sdk");
const bigInt = require("big-integer");
const ndjson = require('ndjson');
const ndjsonParser = require('ndjson-parse');
const lambda = new AWS.Lambda({
  region: "us-west-2",
});

const { Pool, Client } = require("pg");
const copyFrom = require("pg-copy-streams").from;

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
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
  table_to_save_listings: tableToSaveListings,
} = require("./controllers/listings_update_reference.controller");

const {
  metaCreate,
  metaDeleteAll,
  metaDataExists,
  ismetadataNew,
  is_meta_data_new,
} = require("./controllers/listings_meta.controller");

const { metaURL, replicationURL, token } = require("./config/url");
const { response } = require("express");
const tbl_listings_meta = "listings_meta";

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
  });
};

const readWriteListingData = async (values) => {
  // Get inputStream from replication request with range headers
  var stream1 = request({
    url: replicationURL,
    headers: {
      Accept: "application/json",
      Authorization: "Bearer " + token,
      "If-Range": values.ETag,
      Range: "sequence=" + values.startSequence + "-" + values.endSequence,
    },
  });

  const uploadStream = ({ Bucket, Key }) => {
    const s3 = new AWS.S3();
    const pass = new stream.PassThrough();
    return {
      writeStream: pass,
      promise: s3.upload({ Bucket, Key, Body: pass }).promise(),
    };
  };

  const params = {
    Bucket: "listhubdev",
    key: "propertylisting.json",
  };

  const { writeStream, promise } = uploadStream(params);

  return new Promise((resolve, reject) => {
    const pipeline = stream1.pipe(writeStream);

    pipeline.on("close", () => {
      console.log("upload successful");
      resolve({ savedData: true });
    });
    pipeline.on("error", (err) => {
      console.log("upload failed", err.message);
      resolve({ savedData: false });
    });
  });
};

const setMetaTable = async (meta_table) => {
  try {
    const client = await pool.connect();

    //id, last_modifed, content_length, etag, content_type
    // Get list_a_time_modifed
    client.query(
      `DROP TABLE IF EXISTS ${meta_table} CASCADE`,
      (err, result) => {
        if (err) {
          console.log(err);
          return { table_created: false };
        } else {
          console.log(`Table ${meta_table} deleted successfully`);

          client.query(
            `CREATE TABLE IF NOT EXISTS ${meta_table}(id SERIAL PRIMARY KEY, last_modifed TEXT, content_length BIGINT, etag TEXT UNIQUE, content_type TEXT)`,
            (err, result) => {
              if (err) {
                console.log(err);
              } else {
                console.log(`Table ${meta_table} created successfully"`);

                return { table_created: true };
              }
            }
          ); // End of Create Table
        }
      }
    );
  } catch (err) {
    console.log("Create table listings error" + err);
    // return ({updated:false, data: result.rows[0], error:err})
  }
};

const setListingsTable = async (table_to_set) => {
  try {
    const client = await pool.connect();

    return new Promise((resolve, reject) => {
      // Get list_a_time_modifed
      client.query(
        `DROP TABLE IF EXISTS ${table_to_set} CASCADE`,
        (err, res) => {
          if (err) {
            console.log(err);
            resolve({ table_created: false });
          } else {
            console.log(`Table ${table_to_set} deleted successfully`);

            client.query(
              `CREATE TABLE IF NOT EXISTS ${table_to_set}(sequence TEXT, property JSON)`,
              (err, res) => {
                if (err) {
                  console.log(err);
                } else {
                  console.log(`Table ${table_to_set} created successfully"`);

                  resolve({ table_created: true });
                }
              }
            ); // End of Create Table
          }
        }
      );
    }); // End of Promise
  } catch (err) {
    console.log("Create table listings error" + err);
    return { table_created: false, data: null, error: err };
  }
};

const createNewMetaData = async (data) => {
  console.log("Inside create new metadata");

  try {
    const client = await pool.connect();

    return new Promise((resolve, reject) => {
      client.query(
        `INSERT INTO ${tbl_listings_meta} (id, last_modifed, content_length, etag, content_type) VALUES (DEFAULT, $1,$2,$3,$4) RETURNING id`,
        [data.LastModified, data.ContentLength, data.ETag, data.ContentType],
        (err, res) => {
          if (err) {
            console.log(err);
            resolve({
              metadataAdded: false,
              error: "Could Not add Data",
              metadata: null,
            });
          } else {
            console.log("row inserted with id: " + res.rows[0].id);

            resolve({ metadataAdded: true, metadata: data, error: null });
          }
        }
      );
    }); // End of Promise
  } catch (err) {
    console.log("Error " + err);

    return {
      metadataAdded: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Could not create the PropertyMeta.",
      error: err,
      metadata: null,
    };
  }
};

const metaDataExist = async () => {
  const tbl_listings_meta = "listings_meta";

  try {
    const client = await pool.connect();

    return new Promise((resolve, reject) => {
      client.query(`SELECT * from ${tbl_listings_meta}`, (err, res) => {
        if (err) {
          console.log("Check error" + err);

          resolve({
            dataExists: false,
            metadata: null,
            error: err,
            statusCode: null,
            headers: null,
            body: "",
          });
        }

        // console.log("Rows: "+JSON.stringify(res))

        if (res.rowCount != 0) {
          console.log("Meta Data does exist");

          resolve({
            dataExists: true,
            metadata: res.rows,
            error: null,
            statusCode: 200,
            headers: null,
            body: "Successfully created data",
          });
        } else {
          console.log("Meta Data does not exist");

          resolve({
            dataExists: false,
            metadata: null,
            error: null,
            statusCode: 500,
            headers: null,
            body: "Successfully created data",
          });
        }
      });
    });
  } catch (err) {
    console.log("Error in meta" + err);

    return {
      dataExists: false,
      metadata: null,
      error: err,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem finding PropertyMeta Info.",
    };
  }
};

const invokeStreamExecutor = (payload) => {
  const params = {
    FunctionName: "listhub-replica-dev-streamExecutor1",
    InvocationType: "Event", 
    Payload: payload,
  };

  return new Promise((resolve, reject) => {
    lambda.invoke(params, (error, data) => {
      if (error) {
        reject(error);
      } else if (data) {
        resolve(data);
      }
    });
  });
};

const getRangesFromMetadata = (metadata, chunkSize = 20000) => {
  const ETag = metadata.ETag;
  const lastSequence = bigInt(metadata.Metadata.lastsequence.toString());
  const count = metadata.Metadata.totallinecount;
  const firstSequence = lastSequence.minus(count).add(1);
  let rangeFirstSequence = firstSequence;
  let ranges = [];

  while (1) {
    if (rangeFirstSequence.add(chunkSize).gt(lastSequence)) {
      const start = rangeFirstSequence.toString();
      const end = lastSequence.toString();

      ranges.push({ start: start, end: end, ETag: ETag });

      break;
    } else {
      const start = rangeFirstSequence.toString();
      const end = rangeFirstSequence.add(chunkSize).toString();

      ranges.push({ start: start, end: end, ETag: ETag });
    }

    rangeFirstSequence = rangeFirstSequence.add(chunkSize).add(1);
  }

  return ranges;
};

/**
 *  ListHubMonitor
 *  Invokes every 1 hour and detects any update.
 */
module.exports.listhubMonitor = async (event, context) => {
  try {
    const table_a = "listhub_listings_a";
    const table_b = "listhub_listings_b";
    const meta_table = "listings_meta";

    const { table_created } = await setListingsTable(table_a);
    console.log(table_a + " created " + table_created);

    await setListingsTable(table_b);
    await setMetaTable(meta_table);

    // Get meta_data info
    const response = await getMetaDataStream();

    if (response) {
      // Check whether there is new meta_data
      const { dataExists } = await metaDataExist();

      console.log("Data: dataExists " + dataExists);
      // Store meta_data if none exists
      if (!dataExists) {
        // Store the new Metadata
        const { metadataAdded } = await createNewMetaData(response.data);

        console.log("Meta Data Added: " + metadataAdded);

        // Check if meta_data has been stored for the first time
        if (metadataAdded) {
          console.log("New metadata has been created");

          const ranges = getRangesFromMetadata(response.data);
          console.log("Ranges.length " + ranges.length);

          // Download new listings by calling StreamExecutor with table_name and ranges
          // We shall download to two tables at the same time
          for (let index = 0; index < ranges.length; index++) {
            let range = ranges[index];
            
            console.log("Start - " + range.start + " End - " + range.end + " ETag" + range.ETag);

            await invokeStreamExecutor(JSON.stringify({ range, table_name: table_a }));
          }
        } else {
          console.log("Problem creating meta Data. Please try later");
        }
      } else {
        // Compare stored meta_data and new meta_data coming in from listhub to see if we have new listings
        const { newUpdate } = await is_meta_data_new(response.data.Metadata.lastmodifiedtimestamp);

        if (newUpdate) {
          // Delete old meta and Download new Meta Data

          const { metadataDeleted, error } = await metaDeleteAll();

          if (metadataDeleted) {
            const { metadataAdded } = await createNewMetaData(response.data);

            // Check which table to save new data
            const { table_to_save } = await tableToSaveListings();

            const ranges = getRangesFromMetadata(response.data);
            console.log("Ranges.length " + ranges.length);
  
            // Download new listings by calling StreamExecutor with table_name and ranges
            // We shall download to two tables at the same time
            for (let index = 0; index < ranges.length; index++) {
              let range = ranges[index];
              
              console.log("Start - " + range.start + " End - " + range.end + " ETag" + range.ETag);
  
              await invokeStreamExecutor(JSON.stringify({ range, table_name: table_to_save }));
            }
          }
        }
      }
    }
  } catch (err) {
    console.log("Error: " + err);
  }
};

module.exports.streamExecutor1 = async (event, context, callback) => {
  console.log('streamExecutor1 called');
  const promise = new Promise((resolve) => {
    setTimeout(() => {
      console.log('streaming finished');
      resolve();
    }, 60000);
  });

  await promise;
};

/**
 * StreamExecutor
 * Streams the range and adds all listings in that range to the database
 */
module.exports.streamExecutor = async (event, context, callback) => {
  console.log("From List Hub Monitor " + JSON.stringify(event));
  console.log("ETag " + event.range.ETag);
  console.log("Start " + event.range.start);
  console.log("End " + event.range.end);
  console.log("Table_Name " + event.table_name);

  const ETag = event.range.ETag;
  const start = event.range.start;
  const end = event.range.end;
  const table_name = event.table_name;
  const listingArray = [];

  // Get inputStream from replication request with range headers
  const stream = request({
    url: replicationURL,
    headers: {
      Accept: "application/json",
      Authorization: "Bearer " + token,
      "If-Range": ETag,
      Range: "sequence=" + start + "-" + end,
    },
  });
  
  
  const streamingPromise = new Promise((resolve, reject) => {
    // STREAMING WITH JSON STREAM
    console.log("Start Time: " + new Date());

    /*stream.pipe(JSONStream.parse()).pipe(
      es.mapSync((data) => {
        listingArray.push(data);
        console.log("Data Sequence " + JSON.stringify(data) )
      })
    );*/

    stream.pipe(ndjson.parse())
    .on('data', (data) => {
      listingArray.push(data);
      console.log("Data Sequence " + data.sequence)
      // obj is a javascript object
    })

    stream
      .on("complete", async () => {
        
        console.log(
          "Completed reading API range, Data to save is: " +
            listingArray.length +
            " records"
        );

        const client = await pool.connect();

        const dbOperationPromise = new Promise((resolve, reject) => {
          const promises = listingArray.map((listing) => new Promise((resolve, reject) => {
            client.query(
              `INSERT INTO ${table_name} (sequence, Property) VALUES ($1,$2) RETURNING sequence`,
              [listing.sequence, listing.Property],
              (err) => {
                if (err) {
                  console.log(err);
                  reject();
                } else {
                  resolve();
                }
              }
            );
          }));

          Promise.all(promises).then(resolve).catch(reject);
        });

        try {
          await dbOperationPromise;
          console.log('Listings are added successfully!');
          resolve();
        } catch (error) {
          console.log('Something went wrong while adding the listings', error);
          reject(error);
        }
      })
      .on("error", (err) => {
        console.log("Error in request" + err);
        reject(err);
      });
  });

  await streamingPromise;

};

module.exports.checkDataInTables = async () => {
  // Read the json data one by one and compare to see if it is in database and confirm

  // Read the listings from the database

  const client = await pool.connect();

  await client.query(`SELECT * FROM ${table_a}`, (err, res) => {
    if (err) {
      console.log(err);
    }
    if (res.rowCount > 0) {
      console.log("Found - " + res.rowCount + " records");
    }
  });

  //console.log(" Items checked "+fileCount + "Items Found" + foundCount + " Duplicates Found" + duplicateFound)
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
      context.done(null, "FAILURE");
    })
    .on("finish", () => {
      context.succeed("Sucess");

      /*
      client.query(`INSERT INTO ${table_name} (sequence,Property) VALUES ($1,$2) RETURNING sequence`, 
                      [listArray[i].sequence, listArray[i].Property], (err, result) => {
                          
                          if (err) {
                              console.log(err);
                          } else {
                              //console.log('row inserted with : ' + result.rows[0].sequence);
                          }
          
                          count++;
                          
                          if (count == listArray.length) {

                            console.log("Start Sequence: " + startSequence + "End Sequence: "+endSequence + "Added")
                            console.log("Records added - "+count)
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
      */
    });
};
