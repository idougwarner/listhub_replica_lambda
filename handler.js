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
const client = await pool.connect();

const { metaURL, replicationURL, token } = require("./config/url");

const listhubReplicaTableName = "listhub_replica";
const listhubListingsATableName = "listhub_listings_a";
const listhubListingsBTableName = "listhub_listings_b";

const sendQuery = (query, variables) => new Promise((resolve, reject) => {
  const callback = (error, res) => {
    if (error) {
      reject(error);
    } else {
      resolve(res);
    }
  };

  if (!variables) {
    client.query(query, callback);
  } else {
    client.query(query, variables, callback);
  }
});

const createReplicaTable = async (dropFirst = false) => {
  try {
    if (dropFirst) {
      await sendQuery(`DROP TABLE IF EXISTS ${listhubReplicaTableName} CASCADE`);
    }

    await sendQuery(`CREATE TABLE IF NOT EXISTS ${listhubReplicaTableName}(id SERIAL PRIMARY KEY, last_modified TIMESTAMP, table_recent VARCHAR(20), table_stale VARCHAR(20), jobs_count INT, fulfilled_jobs_count INT, syncing BOOLEAN, created_at TIMESTAMP)`);

    console.log(`${listhubReplicaTableName} created successfully!`);
  } catch (error) {
    console.log('createReplicaTable error', error);
  }
}

const createListingsTables = async (dropFirst = false) => {
  try {
    if (dropFirst) {
      await sendQuery(`DROP TABLE IF EXISTS ${listhubListingsATableName} CASCADE`);
      await sendQuery(`DROP TABLE IF EXISTS ${listhubListingsBTableName} CASCADE`);
    }

    await sendQuery(`CREATE TABLE IF NOT EXISTS ${listhubListingsATableName}(id SERIAL PRIMARY KEY, sequence VARCHAR (30), property JSON)`);
    await sendQuery(`CREATE TABLE IF NOT EXISTS ${listhubListingsBTableName}(id SERIAL PRIMARY KEY, sequence VARCHAR (30), property JSON)`);
  } catch (error) {
    console.log('createListingsTables error', error);
  }
};

const createListingsTable = async (name, dropFirst = false) => {
  try {
    if (dropFirst) {
      await sendQuery(`DROP TABLE IF EXISTS ${name} CASCADE`);
    }

    await sendQuery(`CREATE TABLE IF NOT EXISTS ${name}(id SERIAL PRIMARY KEY, sequence VARCHAR (30), property JSON)`);
  } catch (error) {
    console.log('createListingsTables error', error);
  }
};

const addSyncMetadata = async ({
  lastModified,
  tableRecent,
  tableStale,
  jobsCount,
  fulfilledJobsCount = 0,
  syncing = true
}) => {
  try {
    const query = `INSERT INTO ${listhubReplicaTableName} (last_modified, table_recent, table_stale, jobs_count, fulfilled_jobs_count, syncing, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`;
    const variables = [lastModified, tableRecent, tableStale, jobsCount, fulfilledJobsCount, syncing, new Date()];

    return sendQuery(query, variables);
  } catch (error) {
    console.log('addReplicaMeta error', error);
  }
};

const getLastSyncMetadata = async() => {
  try {
    const result = await sendQuery(`SELECT * from ${listhubReplicaTableName} ORDER BY created_at DESC LIMIT 1`);
    return result.rows[0];
  } catch (error) {
    console.log('getLastSyncMetadata error', error);
    return null;
  }
};

const increaseJobCount = async (id) => {
  const readQuery = `SELECT fulfilled_jobs_count from ${listhubReplicaTableName} where id = $1 FOR UPDATE`;
  const updateQuery = `UPDATE ${listhubReplicaTableName} SET fulfilled_jobs_count = $1 WHERE id = $2 RETURNING id`;

  try {
    await client.query('BEGIN');
    const result = await client.query(readQuery, [id]);
    console.log('increase job count', result);
    await client.query(updateQuery, [result.rows[0].fulfilled_jobs_count + 1, id])
    await client.query('COMMIT');
  } catch (error) {
    await cllient.query('ROLLBACK');
  }

  // // select fulfilled job count and increment by one then update the table in transaction mode
  // const result = await client.query(`SELECT * FROM ${listhubReplicaTableName} ORDER BY time_stamp DESC`);

  // return new Promise((resolve, reject) => {
  //   if (result.rowCount > 0) {
  //     var id = result.rows[0].id;
  //     var fulfilled_jobs_count = result.rows[0].fulfilled_jobs_count;
  //     fulfilled_jobs_count = parseInt(fulfilled_jobs_count) + 1;

  //     client.query("BEGIN", (err) => {
  //       if (err) {
  //         console.log("Did not manage to begin adding count" + err);
  //         reject();
  //       }
  //       client.query(
  //         `LOCK TABLE ${listhubReplicaTableName} IN ROW EXCLUSIVE MODE`,
  //         (err, res) => {
  //           if (err) {
  //             console.log("Did not manage to lock table: " + err);
  //             reject();
  //           }
  //           client.query(
  //             `UPDATE ${listhubReplicaTableName} SET fulfilled_jobs_count=$1 WHERE id=$2 RETURNING *`,
  //             [fulfilled_jobs_count, id],
  //             (err, res) => {
  //               if (err) {
  //                 console.log("Error adding job count " + err);
  //                 client.query("ROLLBACK");
  //                 client.release();
  //                 reject();
  //               } else {
  //                 client.query("COMMIT");
  //                 client.release();
  //                 resolve({ increasedJobCount: true });
  //               }
  //             }
  //           );
  //         }
  //       );
  //     });
  //   } else {
  //     console.log(`No data in ${listhubReplicaTableName} to update`);
  //     reject();
  //   }
  // });
};

// Get inputStream from replication request with range headers
const getMetaDataStream = () => axios({
  url: metaURL,
  method: "get",
  headers: {
    Accept: "application/json",
    Authorization: "Bearer " + token,
  },
});

const checkIfListhubUpdated = (metadata, lastSyncMetadata) => {
  const newTime = new Date(metadata.Metadata.lastmodifiedtimestamp);
  const storedTime = new Date(lastSyncMetadata.last_modified);

  return newTime > storedTime;
};

const createListhubReplicaMetadata = async (data) => {
  console.log("Inside create new Listhub replica");

  try {
    const client = await pool.connect();

    // Insert new data into the replica

    return new Promise((resolve, reject) => {
      var now = new Date();

      client.query(
        `INSERT INTO ${listhubReplicaTableName} (last_modified, table_recent, table_stale, jobs_count, fulfilled_jobs_count, syncing, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
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
      client.query(`SELECT * from ${listhubReplicaTableName}`, (err, res) => {
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
      client.query(`SELECT * from ${listhubReplicaTableName} ORDER BY created_at DESC`, (err, res) => {
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
  const result = await client.query(`SELECT * FROM ${table_name} LIMIT 50`);

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
  const result = await client.query(`SELECT * FROM ${listhubReplicaTableName} ORDER BY created_at DESC`);

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
      } else if (data) {
        resolve(data);
      }
    });
  });
};

const getRangesFromMetadata = (metadata, chunkSize = 30000) => {
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

const syncListhub = async (metadata, lastSyncMetadata) => {
  const ranges = getRangesFromMetadata(metadata);
  console.log("Ranges.length " + ranges.length);

  for (let index = 0; index < ranges.length; index++) {
    let range = ranges[index];

    console.log(`Range: ${range.start} - ${range.end}`);

    try {
      await invokeStreamExecutor({
        range: range,
        lastSyncMetadataId: lastSyncMetadata.id,
        tableName: lastSyncMetadata.table_recent,
      });
    } catch (error) {
      console.log("syncListhub error", error);
    }
  }
};

module.exports.prepareListhubTables = async (event, context) => {
  await createListingsTables(true);
  await createReplicaTable(true);
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
      const metadata = response.data;

      let lastSyncMetadata = await getLastSyncMetadata();

      if (lastSyncMetadata && !checkIfListhubUpdated(metadata, lastSyncMetadata)) return;

      const ranges = getRangesFromMetadata(metadata);

      lastSyncMetadata = await addSyncMetadata({
        lastModified: metadata.Metadata.lastmodifiedtimestamp,
        tableRecent: lastSyncMetadata ? lastSyncMetadata.table_stable : listhubListingsATableName,
        tableStale: lastSyncMetadata ? lastSyncMetadata.table_recent : listhubListingsBTableName,
        jobsCount: ranges.length
      });
      console.log('last sync data', lastSyncMetadata);
      await createListingsTable(lastSyncMetadata.table_recent);

      await syncListhub(metadata, lastSyncMetadata);
      // // Check whether there is new data by comparing what is in listhub_replica last_modified versus what is in the metadata
      // const { dataExists } = await listhubDataExists();

      // console.log("Data: dataExists " + dataExists);
      // console.log("Metadata Data " + JSON.stringify(response.data));

      // // Store new listhub replica data if none exists
      // if (!dataExists) {
      //   // Call SyncListhub with metadata and create listings a table
      //   await syncListhub(response.data, listhubListingsATableName);
      // } else {
      //   // Check if listhub_listings_b has data, if not populate it with data
      //   const { hasdata } = await tableHasListings(listhubListingsBTableName);

      //   if (!hasdata) {
      //     // This will in turn make it the recent table
      //     console.log("Listings_b has no data therefore populate it")
      //     await syncListhub(response.data, listhubListingsBTableName);
      //   }
      //   else {
      //     const { newUpdate } = await isMetadataNew(
      //       response.data.Metadata.lastmodifiedtimestamp
      //     );

      //     if (newUpdate) {
            
      //       // Update listhub_replica data with new timestamp and check which table to now set data to
      //       const { table_to_save } = await tableToSaveListings();

      //       // Clear data from the table if existing data then save data
      //       const { deleted, tableOk } = await clearDataFrom(table_to_save);
      //       if (deleted || tableOk) {
      //         await syncListhub(response.data, table_to_save);
      //       }

      //     }
      //   }   
      // }
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
  console.log("ETag " + event.range.ETag);
  console.log("Start " + event.range.start);
  console.log("End " + event.range.end);
  console.log("Table_Name " + event.tableName);
  console.log('Last sync meta data id: ', event.lastSyncMetadataId)

  const ETag = event.range.ETag;
  const start = event.range.start;
  const end = event.range.end;
  const tableName = event.tableName;
  const lastSyncMetadataId = event.lastSyncMetadataId;
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
    console.log("Start Time: " + new Date());

    stream.pipe(ndjson.parse()).on("data", (data) => {
      listingArray.push(data);
    });

    stream
      .on("complete", async () => {
        console.log(`Streamed records count: ${listingArray.length}`);

        // // Check if there is a change in ETag by getting a status code of 412
        // if (listingArray.length == 1) {
        //   console.log("Error Result " + JSON.stringify(listingArray));

        //   // We should handle this error in the midst of our fetching of data
        //   if (listingArray[0].statusCode == 412) {
        //     console.log(
        //       "We may need to restart our fetching of data as the server has changed the ETag as we fetch listings"
        //     );
        //     reject({ "addedjobcount": false, "listingdata": false });
        //   }
        // }

        let listingsCount = listingArray.length;

        if (true) {
          listingsCount = 10;
        }

        for (let index = 0; index < listingsCount; index += 1) {
          const listing = listingArray[index];
          const query = `INSERT INTO ${tableName} (sequence, Property) VALUES ($1,$2) RETURNING sequence`;
          const variables =  [listing.sequence, listing.Property];
          await sendQuery(query, variables);
        }

        stream.end();

        resolve();
      })
      .on("error", (err) => {
        console.log("Error in getting listings" + err);
        stream.end();
        reject();
      });
  });

  try {
    await streamingPromise;
    await increaseJobCount(lastSyncMetadataId);
  } catch (error) {
    
  }
};

module.exports.monitorSync = async () => {
  try {
    const client = await pool.connect();
    const result = await client.query(
      `SELECT * from ${listhubReplicaTableName} ORDER BY created_at DESC`
    );

    console.log("Checking data...\n");

    if (result.rowCount > 0) {
      if (
        (result.rows[0].syncing) &&
        result.rows[0].jobs_count == result.rows[0].fulfilled_jobs_count
      ) {
        console.log("Data will be synced...\n");

        const id = result.rows[0].id;

        client.query(
          `UPDATE ${listhubReplicaTableName} SET syncing=$1 WHERE id=$2 RETURNING *`,
          [false, id],
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
      }else if (
        (result.rows[0].syncing) &&
         result.rows[0].fulfilled_jobs_count < result.rows[0].jobs_count
      ) {

        console.log("Data is still syncing...\n");

      } 
      else if (
        (result.rows[0].syncing==false) &&
        result.rows[0].jobs_count == result.rows[0].fulfilled_jobs_count
      ) {

        console.log("Data is already synced...\n");

      } 
    } else {
      console.log("There is no data to sync...");
    }
  } catch (err) {
    console.log("Error in syncing" + err);
  }
};
