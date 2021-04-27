"use strict";
const axios = require("axios");
const request = require("request");
const stream = require("stream");
const https = require("https");
const JSONStream = require("JSONStream");
const es = require("event-stream");
const AWS = require("aws-sdk");
const bigInt = require("big-integer");
const ndjson = require("ndjson");
const lambda = new AWS.Lambda({
  region: "us-west-2",
});
const TimeUtil = require("./utils/timeFunctions");

const { Pool } = require("pg");

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
});

const { metaURL, replicationURL, token } = require("./config/url");

const tbl_listhub_replica = "listhub_replica";
const listings_a = "listhub_listings_a";
const listings_b = "listhub_listings_b";

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

const createListhubReplicaTable = async () => {
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
    return new Promise((resolve, reject) => {
      client.query(
        `DROP TABLE IF EXISTS ${tbl_listhub_replica} CASCADE`,
        (err, result) => {
          if (err) {
            console.log(err);
            resolve({ listhub_table_created: false });
          } else {
            console.log(`Table ${tbl_listhub_replica} deleted successfully`);

            client.query(
              `CREATE TABLE IF NOT EXISTS ${tbl_listhub_replica}(id SERIAL PRIMARY KEY, last_modified TIMESTAMP, table_recent TEXT, table_stale TEXT, jobs_count BIGINT, fulfilled_jobs_count BIGINT, syncing BOOLEAN, time_stamp TIMESTAMP)`,
              (err, result) => {
                if (err) {
                  console.log(err);
                } else {
                  console.log(
                    `Table ${tbl_listhub_replica} created successfully"`
                  );

                  resolve({ listhub_table_created: true });
                }
              }
            ); // End of Create Table
          }
        }
      );
    });
  } catch (err) {
    console.log(`Create ${tbl_listhub_replica} listings error` + err);
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
            resolve({ table_created: false, table_name: table_to_set });
          } else {
            console.log(`Table ${table_to_set} deleted successfully`);

            client.query(
              `CREATE TABLE IF NOT EXISTS ${table_to_set}(id SERIAL PRIMARY KEY, sequence TEXT, property JSON)`,
              (err, res) => {
                if (err) {
                  console.log(err);
                } else {
                  console.log(`Table ${table_to_set} created successfully"`);

                  resolve({ table_created: true, table_name: table_to_set });
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

const createListhubReplicaMetadata = async (data) => {
  console.log("Inside create new Listhub replica");

  try {
    const client = await pool.connect();

    // Insert new data into the replica

    return new Promise((resolve, reject) => {
      var now = new Date();

      client.query(
        `INSERT INTO ${tbl_listhub_replica} (last_modified, table_recent, table_stale, jobs_count, fulfilled_jobs_count, syncing, time_stamp) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
        [
          data.last_modified,
          data.table_recent,
          data.table_stale,
          data.jobs_count,
          data.fulfilled_jobs_count,
          data.syncing,
          now,
        ],
        (err, res) => {
          if (err) {
            console.log(err);
            resolve({
              metadataAdded: false,
            });
          } else {
            console.log("Meta data row inserted with id: " + res.rows[0].id);
            console.log("Metadata: " + JSON.stringify(res.rows));

            resolve({ metadataAdded: true });
          }
        }
      );
    });
  } catch (err) {
    console.log("Error " + err);

    return {
      metadataAdded: false,
    };
  }
};

const listhubDataExists = async () => {
  try {
    const client = await pool.connect();

    return new Promise((resolve, reject) => {
      client.query(`SELECT * from ${tbl_listhub_replica}`, (err, res) => {
        if (err) {
          console.log("Check error" + err);

          resolve({
            dataExists: false,
          });
        }

        if (res.rowCount != 0) {
          console.log("Listhub does exist");

          resolve({
            dataExists: true,
          });
        } else {
          console.log("Meta Data does not exist");

          resolve({
            dataExists: false,
          });
        }
      });
    });
  } catch (err) {
    console.log("Error in meta" + err);

    resolve({
      dataExists: false,
    });
  }
};

const isMetadataNew = async (newtime) => {
  try {
    const client = await pool.connect();

    return new Promise((resolve, reject) => {
      client.query(`SELECT * from ${tbl_listhub_replica}`, (err, res) => {
        if (res.rowCount > 0) {
          var storedTime = res.rows[0].last_modified;

          let timeResult = TimeUtil.istimeANewerthantimeB(newtime, storedTime);

          if (timeResult.newUpdate) {
            client.release();

            resolve({ newUpdate: true, error: null });
          } else {
            client.release();

            resolve({ newUpdate: false, error: "No Update" });
          }
        } else {
          reject();
        }
      });
    });
  } catch (err) {
    const result = {
      newUpdate: false,
    };

    return result;
  }
};

const tableHasListings = async (table_name) => {
  const client = await pool.connect();
  const result = await client.query(`SELECT * FROM ${table_name}`);

  return new Promise((resolve, reject) => {
    if (result.rowCount > 0) {
      resolve({ hasdata: true });
    } else {
      resolve({ hasdata: false });
    }
  });
};

const clearDataFrom = async (table_name) => {
  const client = await pool.connect();
  const result = await client.query(`SELECT * FROM ${table_name}`);

  return new Promise((resolve, reject) => {
    if (result.rowCount > 0) {
      // Delete all from table
      client.query(`DELETE * FROM ${table_name}`, (err, rslt) => {
        if (rslt.rowCount > 0) {
          resolve({ deleted: true, tableOk: true });
        } else {
          resolve({ deleted: false, tableOk: false });
        }
      });
    } else {
      resolve({ deleted: false, tableOk: true });
    }
  });
};

const tableToSaveListings = async () => {
  var table_stale;

  const client = await pool.connect();
  const result = await client.query(
    `SELECT * FROM ${tbl_listhub_replica} ORDER BY time_stamp DESC`
  );

  return new Promise((resolve, reject) => {
    if (result.rowCount > 0) {
      table_stale = result.rows[0].table_stale;
      resolve({ table_to_save: table_stale });
    }
  });
};

const invokeStreamExecutor = async (payload) => {
  const params = {
    FunctionName: "listhub-replica-dev-streamExecutor",
    InvocationType: "Event",
    Payload: JSON.stringify(payload),
  };

  return new Promise((resolve, reject) => {
    lambda.invoke(params, (error, data) => {
      if (error) {
        reject(error);
        console.error("Error in call table_a: " + JSON.stringify(error));

        return new Error(`Error printing messages: ${JSON.stringify(error)}`);
      } else if (data) {
        resolve(data);
        console.log("table_a_results" + JSON.stringify(data));
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

const didDownloadFinish = async () => {
  const client = await pool.connect();
  // await client.query(`SELECT * FROM ${tbl_listhub_replica} WHERE table_stale=$1 ORDER BY time_stamp DESC`,[table_name]);
  const result = await client.query(
    `SELECT * FROM ${tbl_listhub_replica} ORDER BY time_stamp DESC`
  );

  return new Promise((resolve, reject) => {
    if (result.rowCount > 0) {
      var fulfilled_jobs_count = result.rows[0].fulfilled_jobs_count;
      var jobs_count = result.rows[0].jobs_count;
      var syncing = result.rows[0].syncing;
      var table_recent = result.rows[0].table_recent;

      if (jobs_count == fulfilled_jobs_count && syncing == "false") {
        console.log(
          "Download completed successfully",
          syncing == "false",
          jobs_count
        );
        resolve({ downloadFishished: true, tableRecent: table_recent });
      } else {
        console.log("Downloads did not complete");
        resolve({ downloadFishished: false, tableRecent: table_recent });
      }
    }
  });
};

const syncListhub = async (metadata, targetTable) => {
  const ranges = getRangesFromMetadata(metadata);
  console.log("Ranges.length " + ranges.length);
  console.log("Last modified time " + metadata.Metadata.lastmodifiedtimestamp);
  var data;

  // Store listhub replica data to the table prioritizing the table
  if (targetTable == listings_a) {
    data = {
      last_modified: metadata.Metadata.lastmodifiedtimestamp,
      table_recent: listings_a,
      table_stale: listings_b,
      jobs_count: ranges.length,
      fulfilled_jobs_count: 0,
      syncing: true,
    };
  } else if (targetTable == listings_b) {
    data = {
      last_modified: metadata.Metadata.lastmodifiedtimestamp,
      table_recent: listings_b,
      table_stale: listings_a,
      jobs_count: ranges.length,
      fulfilled_jobs_count: 0,
      syncing: true,
    };
  }

  // Store the new Metadata details to Listhub replica this time
  const { metadataAdded } = await createListhubReplicaMetadata(data);

  // Check if meta_data has been stored for the first time
  if (metadataAdded) {
    console.log("New metadata has been created");

    for (let index = 0; index < ranges.length; index++) {
      let range = ranges[index];

      console.log(`Range: ${range.start} - ${range.end}`);

      try {
        const result = await invokeStreamExecutor({
          range: range,
          table_name: targetTable,
        });
        console.log("streamExecutor is invoked", JSON.stringify(result));
      } catch (error) {
        console.log("streamExecutor invocation error", error);
      }
    }
  } else {
    console.log("Problem creating meta Data. Please try later");
  }
};

const increaseJobCount = async () => {
  // select fulfilled job count and increment by one then update the table in transaction mode
  const client = await pool.connect();
  const result = await client.query(`SELECT * FROM ${tbl_listhub_replica}`);

  return new Promise((resolve, reject) => {
    if (result.rowCount > 0) {
      var id = result.rows[0].id;
      var fulfilled_jobs_count = result.rows[0].fulfilled_jobs_count;
      fulfilled_jobs_count = parseInt(fulfilled_jobs_count) + 1;

      client.query("BEGIN", (err) => {
        if (err) {
          console.log("Did not manage to begin adding count" + err);
          reject();
        }
        client.query(
          `LOCK TABLE ${tbl_listhub_replica} IN ROW EXCLUSIVE MODE`,
          (err, res) => {
            if (err) {
              console.log("Did not manage to lock table: " + err);
              reject();
            }
            client.query(
              `UPDATE ${tbl_listhub_replica} SET fulfilled_jobs_count=$1 WHERE id=$2 RETURNING *`,
              [fulfilled_jobs_count, id],
              (err, res) => {
                if (err) {
                  console.log("Error adding job count " + err);
                  client.query("ROLLBACK");
                  client.release();
                  reject();
                } else {
                  client.query("COMMIT");
                  client.release();
                  resolve({ increasedJobCount: true });
                }
              }
            );
          }
        );
      });
    } else {
      console.log(`No data in ${tbl_listhub_replica} to update`);
      reject();
    }
  });
};

module.exports.prepareListhubTables = async () => {
  const { table_created, table_name } = await setListingsTable(listings_a);
  console.log(table_name + " created? " + table_created);

  await setListingsTable(listings_b);
  //console.log(table_name + " created? " + table_created);

  const { listhub_table_created } = await createListhubReplicaTable();
  console.log("Lishub_replica table" + " created? " + listhub_table_created);
};

/**
 * Lambda handler that invokes every 1 hour to check if ListHub has any updates.
 * This lambda handler allows us to sync our database up with the listhub database.
 */
module.exports.listhubMonitor = async (event, context) => {
  try {
    // Get meta_data info
    const response = await getMetaDataStream();

    if (response) {
      // Check whether there is new data by comparing what is in listhub_replica last_modified versus what is in the metadata
      const { dataExists } = await listhubDataExists();

      console.log("Data: dataExists " + dataExists);
      console.log("Metadata Data " + JSON.stringify(response.data));

      // Store new listhub replica data if none exists
      if (!dataExists) {
        // Call SyncListhub with metadata and create listings a table
        await syncListhub(response.data, listings_a);
      } else {
        // Check if listhub_listings_b has data, if not populate it with data
        const { hasdata } = await tableHasListings(listings_b);

        if (!hasdata) {
          await syncListhub(response.data, listings_b);
        }

        // Check if our most recent syncing finished
        const { downloadFishished, tableRecent } = await didDownloadFinish();

        if (!downloadFishished) {
          await syncListhub(response.data, tableRecent);
        } else {
          // Compare stored listhub replica meta data new meta_data coming in from listhub to see if we have new listings
          const { newUpdate } = await isMetadataNew(
            response.data.Metadata.lastmodifiedtimestamp
          );

          if (newUpdate) {
            // Update listhub_replica data with new timestamp and check which table to now set data to
            const { table_to_save } = await tableToSaveListings();

            // Clear data from the table if existing data then save data
            const { deleted, tableOk } = await clearDataFrom(table_to_save);
            if (deleted || tableOk) {
              await syncListhub(response.data, table_to_save);
            }
          }
        }
      }
    }
  } catch (err) {
    console.log("Error: " + err);
  }
};

/**
 * StreamExecutor
 * Streams the range and adds all listings in that range to the database
 */
module.exports.streamExecutor = async (event, context, callback) => {
  console.log("\n\nFrom List Hub Monitor " + JSON.stringify(event));
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

    stream.pipe(ndjson.parse()).on("data", (data) => {
      listingArray.push(data);
      // console.log("Data Sequence " + data.sequence);
      // obj is a javascript object
    });

    stream
      .on("complete", async () => {
        console.log(
          "Completed reading API range, Data to save is: " +
            listingArray.length +
            " records"
        );

        // Check if there is a change in ETag by getting a status code of 412
        if (listingArray.length == 1) {
          console.log("Error Result " + JSON.stringify(listingArray));

          // We should handle this error in the midst of our fetching of data
          if (listingArray[0].statusCode == 412) {
            console.log(
              "We may need to restart our fetching of data as the server has changed the ETag as we fetch listings"
            );
            reject({ addedjobcount: false, listingdata: false });
          }
        }

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
                      //client.release()
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

          client.release(); // Release connections before more connections to database

          // fulfilled_jobs_count of listhub_replica by 1 in transaction mode
          const { increasedJobCount } = await increaseJobCount();

          if (increasedJobCount) {
            resolve({ addedjobcount: true, listingdata: true });
          } else {
            console.log("Problem with adding job count");
            reject({ addedjobcount: false, listingdata: true });
          }
        } catch (error) {
          console.log(
            "Something went wrong while increasing the job count in listhub_replica",
            error
          );
          reject(error);
        }
      })
      .on("error", (err) => {
        console.log("Error in getting listings" + err);
        stream.end();
        reject({ addedjobcount: false, listingdata: false });
      });
  });

  const { addedjobcount, listingdata } = await streamingPromise;

  if (addedjobcount && listingdata) {
    const response = {
      statusCode: 200,
      body: JSON.stringify({
        message: `${event} - data added well`,
      }),
    };
    stream.end();
    callback(null, response);
  } else {
    const response = {
      statusCode: 500,
      body: JSON.stringify({
        message: `${event} - problem adding data`,
      }),
    };
    stream.end();
    callback(null, response);
  }
};

module.exports.monitorSync = async () => {
  try {
    const client = await pool.connect();
    const result = await client.query(
      `SELECT * from ${tbl_listhub_replica} ORDER BY time_stamp DESC`
    );

    console.log("Checking data...\n");

    if (result.rowCount > 0) {
      if (
        (result.rows[0].syncing === "true" || result.rows[0].syncing) &&
        result.rows[0].jobs_count == result.rows[0].fulfilled_jobs_count
      ) {
        console.log("Data will be synced...\n");

        const id = result.rows[0].id;

        client.query(
          `UPDATE ${tbl_listhub_replica} SET syncing=$1 WHERE id=$2 RETURNING *`,
          ["false", id],
          (err, res) => {
            if (err) {
              console.log(err);

              console.log("Problem with syncing data, trying abit later");
            } else if (res.rows[0]) {
              console.log("Listing data has been synced...");
            } else {
              console.log("Problem syncing data...");
            }
          }
        );
      } else {
        console.log("Data is already synced...");
      }
    } else {
      console.log("There is no data to sync...");
    }
  } catch (err) {
    console.log("Error in syncing" + err);
  }
};
