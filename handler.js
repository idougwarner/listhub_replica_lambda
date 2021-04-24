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
const ndjson = require("ndjson");
const ndjsonParser = require("ndjson-parse");
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
  listDeleteAll
} = require("./controllers/listings_update_reference.controller");

const {
  metaCreate,
  metaDeleteAll,
  ismetadataNew,
  is_meta_data_new: isMetaDataNew,
} = require("./controllers/listings_meta.controller");

const { metaURL, replicationURL, token } = require("./config/url");
const { response } = require("express");
const { reject } = require("async");
const tbl_listings_meta = "listings_meta";
const tbl_listhub_replica = "listhub_replica";
const listings_a = "listhub_listings_a";
const listings_b = "listhub_listings_b";
const meta_table = "listings_meta";


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

const create_listhub_replica_table = async () => {

      /*id
      last_modified
      table_recent (one of listhub_listings_a and listhub_listings_b)
      table_stale (one of listhub_listings_a and listhub_listings_b)
      jobs_count
      fulfulled_jobs_count
      syncing*/

  try {
    const client = await pool.connect();

    // id, last_modifed, content_length, etag, content_type
    // Get list_a_time_modifed
    client.query(
      `DROP TABLE IF EXISTS ${tbl_listhub_replica} CASCADE`,
      (err, result) => {
        if (err) {
          console.log(err);
          return { table_created: false };
        } else {
          console.log(`Table ${tbl_listhub_replica} deleted successfully`);

          client.query(
            `CREATE TABLE IF NOT EXISTS ${tbl_listhub_replica}(id SERIAL PRIMARY KEY, last_modifed TEXT, table_recent TEXT, table_stale TEXT, jobs_count BIGINT, fulfilled_jobs_count BIGINT, syncing TEXT)`,
            (err, result) => {
              if (err) {
                console.log(err);
              } else {
                console.log(`Table ${tbl_listhub_replica} created successfully"`);

                return { table_created: true };
              }
            }
          ); // End of Create Table
        }
      }
    );
  } catch (err) {
    console.log(`Create ${tbl_listhub_replica} listings error` + err);
    // return ({updated:false, data: result.rows[0], error:err})
  }
};

const set_meta_table = async (meta_table) => {
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
              `CREATE TABLE IF NOT EXISTS ${table_to_set}(id SERIAL PRIMARY KEY, sequence TEXT, property JSON)`,
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

const create_listhub_replica_metadata = async (data) => {
  console.log("Inside create new Listhub replica");

  try {
    const client = await pool.connect();

    const results = await db.query(`SELECT * FROM ${tbl_listings_meta}`);
    
    if(results.rowCount>0) {
      var id=results.row[0].id
      
      return new Promise((resolve, reject) => {

        // Check if there is any metadata
        //"UPDATE fishes SET name=$1, type=$2 WHERE id=$3 RETURNING *"
  
        client.query(
          `UPDATE ${tbl_listhub_replica} SET last_modified=$1, table_recent=$2, table_stale=$3, jobs_count=$4, fulfilled_jobs_count=$5, syncing=$6 WHERE id=$6 RETURNING *`,
          [data.last_modifed, data.table_recent, data.table_stale, data.jobs_count, data.fulfilled_jobs_count, data.syncing, id],
          (err, res) => {
            if (err) {
              console.log(err);
              resolve({
                metadataAdded: false,
                metadata: null,
              });
            } else {
              console.log("Meta data row inserted with id: " + res.rows[0].id);
  
              resolve({ metadataAdded: true, metadata: data });
            }
          }
        );
      }); // End of Promise

    }
    else {
      
      return new Promise((resolve, reject) => {

        // Check if there is any metadata
  
        client.query(
          `INSERT INTO ${tbl_listhub_replica} (id, last_modifed, table_recent, table_stale, jobs_count, fulfilled_jobs_count, syncing) VALUES (DEFAULT, $1,$2,$3,$4,$5,$6) RETURNING id`,
          [data.last_modifed, data.table_recent, data.table_stale, data.jobs_count, data.fulfilled_jobs_count, data.syncing],
          (err, res) => {
            if (err) {
              console.log(err);
              resolve({
                metadataAdded: false
              });
            } else {
              console.log("Meta data row inserted with id: " + res.rows[0].id);
  
              resolve({ metadataAdded: true });
            }
          }
        );
      }); // End of Promise

    }
    //id, last_modifed, table_recent, table_stale, jobs_count, fulfilled_jobs_count, syncing

    
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

const listhub_data_exist = async () => {
  
  try {
    const client = await pool.connect();

    return new Promise((resolve, reject) => {
      client.query(`SELECT * from ${tbl_listhub_replica}`, (err, res) => {
        if (err) {
          console.log("Check error" + err);

          resolve ({
            dataExists: false,
          });
        }

        // console.log("Rows: "+JSON.stringify(res))

        if (res.rowCount != 0) {
          console.log("Listhub does exist");

          resolve ({
            dataExists: true,
          });
        } else {
          console.log("Meta Data does not exist");

          resolve ({
            dataExists: false,
          });
        }
      });
    });
  } catch (err) {
    console.log("Error in meta" + err);

    resolve ({
      dataExists: false,
    })
  }
};

const is_meta_data_new = async (newtime) => {

  try {
    const client= await pool.connect()

    return new Promise((resolve, reject) => {
      client.query(`SELECT * from ${tbl_listhub_replica}`, (err, res) => {
        
        if(res.rowCount>0) {
  
          var storedTime = res.row[0].last_modified
          
          let timeResult = TimeUtil.istimeANewerthantimeB(
            newtime,
            storedTime
          );
    
          console.log("TimeResult"+JSON.stringify(timeResult))
    
          if (timeResult.newUpdate) {
  
            client.release()

            resolve ({ newUpdate: true, error: null })            
    
          } else {
             
            client.release()

            resolve ({ newUpdate: false, error: "No Update" })
    
          }
        }
        else {
          reject()
        }

      });

    })
  }
  catch(err) {
    const result = {
      newUpdate: false
    };

    return result;    
  }

}

const clear_data_from = async (table_name) => {

  const client = await pool.connect()
  const result = await client.query(`SELECT * FROM ${table_name}`);

  return new Promise((resolve, reject) => {
    
    if(result.rowCount>0)
    {
      // Delete all from table
      const rslt = await db.query(`DELETE * FROM ${table_name}`);
      if(rslt.rowCount>0)
      {
        resolve({ deleted:true, tableOk: true })
      }
      else {
        resolve({ deleted:false, tableOk: false })
      }
    }
    else {
      resolve({ deleted:false, tableOk: true })
    }
  })

}

const table_to_save_listings = async () => {

  var table_stale;

  const client = await pool.connect()
  const result = await client.query(`SELECT * FROM ${tbl_listhub_replica}`);

  return new Promise((resolve, reject) => {
    
    if(result.rowCount>0)
    {
      table_stale = result.rows[0].table_stale;
      ({table_to_save:table_stale})
    }

    })
}

const invokeStreamExecutor = (payload) => {
  const params = {
    FunctionName: "listhub-replica-dev-streamExecutor",
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

const syncListhub = async (metadata, targetTable) => {
  const ranges = getRangesFromMetadata(metadata);
  console.log("Ranges.length " + ranges.length);
  var data;

  // Store listhub replica data to the table prioritizing the table
  if(targetTable==listings_a) {
    
    data =  { 
      last_modified: metadata.Metadata.lastmodifiedtimestamp,
      table_recent: listings_a,
      table_stale: listings_b,
      jobs_count: ranges.length,
      fulfilled_jobs_count: 0,
      syncing: true 
    }

  }
  else if(targetTable==listings_b) {

    data =  { 
      last_modified: metadata.Metadata.lastmodifiedtimestamp,
      table_recent: listings_b,
      table_stale: listings_a,
      jobs_count: ranges.length,
      fulfilled_jobs_count: 0,
      syncing: true 
    }

  }

  // Store the new Metadata details to Listhub replica this time 
  const { metadataAdded } = await create_listhub_replica_metadata(data);

  // Check if meta_data has been stored for the first time
  if (metadataAdded) {
    console.log("New metadata has been created");

    for (let index = 0; index < ranges.length; index++) {
      let range = ranges[index];
  
      console.log(`Range: ${range.start} - ${range.end}`);
  
      await invokeStreamExecutor(
        JSON.stringify({ range, table_name: targetTable })
      );
    }
   
  } else {
    console.log("Problem creating meta Data. Please try later");
  }

};

const increase_job_count = async () => {

  // select fulfilled job count and increment by one then update the table in transaction mode
  const client = await pool.connect()
  const result = await client.query(`SELECT * FROM ${tbl_listhub_replica}`);

  return new Promise((resolve, reject) => {
    
    if(result.rowCount>0)
    {
      var id = result.rows[0].id;
      var fulfilled_jobs_count = result.rows[0].fulfilled_jobs_count;
      fulfilled_jobs_count = fulfilled_jobs_count + 1

      client.query(
        `UPDATE ${tbl_listhub_replica} SET fulfilled_jobs_count=$1 WHERE id=$2 RETURNING *`,
        [fulfilled_jobs_count, id],
        (err, res) => {
          if (err) {
            console.log(err);
            
            resolve({
              increasedJobCount: true
            });

          } else {
            
            resolve({ increasedJobCount: false });
          }
        });
    }

    })
}

/**
 * Lambda handler that invokes every 1 hour to check if ListHub has any updates.
 * This lambda handler allows us to sync our database up with the listhub database.
 */
module.exports.listhubMonitor = async (event, context) => {
  try {
    
    const { table_created } = await setListingsTable(listings_a);
    console.log(listings_a + " created " + table_created);

    await setListingsTable(listings_b);
    await set_meta_table(meta_table);
    await create_listhub_replica_table()

    // Get meta_data info
    const response = await getMetaDataStream();

    if (response) {

      // Check whether there is new data by comparing what is in listhub_replica last_modified versus what is in the metadata
      const { dataExists } = await listhub_data_exist();

      console.log("Data: dataExists " + dataExists);

      // Store new listhub replica data if none exists
      if (!dataExists) {

        // Call SyncListhub with metadata and create listings a table
        await syncListhub(response.data, listings_a);
        
      } else {

        // Compare stored listhub replica meta data new meta_data coming in from listhub to see if we have new listings
        const { newUpdate } = await is_meta_data_new(response.data.Metadata.lastmodifiedtimestamp);

        if (newUpdate) {

          // Update listhub_replica data with new timestamp and check which table to now set data to
          const { table_to_save } = await table_to_save_listings();
          // Clear data from the table if existing data then save data
          const { deleted, tableOk } = await clear_data_from(table_to_save);
          if(deleted || tableOk)
          {
            await syncListhub(response.data, table_to_save);
          }          

        }
      }
    }
  } catch (err) {
    console.log("Error: " + err);
  }
};

module.exports.streamExecutor1 = async (event, context, callback) => {
  console.log("streamExecutor1 called");
  const promise = new Promise((resolve) => {
    setTimeout(() => {
      console.log("streaming finished");
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

    stream.pipe(ndjson.parse()).on("data", (data) => {
      listingArray.push(data);
      console.log("Data Sequence " + data.sequence);
      // obj is a javascript object
    });

    stream
      .on("complete", async () => {
        console.log(
          "Completed reading API range, Data to save is: " +
            listingArray.length +
            " records"
        );

        const client = await pool.connect();

        const dbOperationPromise = new Promise((resolve, reject) => {
          const promises = listingArray.map(
            (listing) =>
              new Promise((resolve, reject) => {
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
              })
          );

          Promise.all(promises).then(resolve).catch(reject);
        });

        try {
          await dbOperationPromise;
          
          console.log("Listings are added successfully!");
          // fulfilled_jobs_count of listhub_replica by 1 in transaction mode
          
          const { increasedJobCount } = await increase_job_count();

          if(increasedJobCount)
          {
            resolve();
          }else {
            console.log("Problem with adding job count")
            reject();
          }
          

        } catch (error) {
          console.log("Something went wrong while adding the listings", error);
          reject(error);
        }
      })
      .on("error", (err) => {
        console.log("Error in getting listings" + err);
        reject(err);
      });
  });

  await streamingPromise;
};

module.exports.monitorSync = async (event, context, callback) => {
  /**
   * Get last record of listhub. If value of syncing is true and jobcount is equal to fulfilled then set syncing to false
   * 
   */

   const monitorSyncPromise;

   try {
    const client = await pool.connect();

    monitorSyncPromise = new Promise((resolve, reject) => {
      client.query(`SELECT * from ${tbl_listhub_replica}`, (err, res) => {
        if (err) {
          console.log("Check error" + err);

          reject ();
        }

        if (res.rowCount != 0) {
          // Check the condition if syncing == true and jobcount is equal 
          // last_modified, table_recent, table_stale, jobs_count, fulfilled_jobs_count, syncing

          var id = res.rows[0].id

          if(res.rows[0].syncing == true && (res.rows[0].jobs_count==res.rows[0].fulfilled_jobs_count)) {
            // Update set syncing to false
            client.query(
              `UPDATE ${tbl_listhub_replica} SET syncing=$1 WHERE id=$2 RETURNING *`,
              [true, id],
              (err, res) => {
                if (err) {
                  console.log(err);
                  
                  reject();
      
                } else {

                  console.log("Data has been synced")

                  resolve();
                }
              });

          }
        } else {
          console.log("Problems accessing listhub data");

          reject ();
        }
      });
    });
  } catch (err) {
    console.log("Error in syncing" + err);

    reject ()
  }

  await monitorSyncPromise;

} 

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
