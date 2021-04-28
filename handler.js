"use strict";
const axios = require("axios");
const request = require("request");
const AWS = require("aws-sdk");
const bigInt = require("big-integer");
const ndjson = require("ndjson");
const lambda = new AWS.Lambda({
  region: "us-west-2",
});

const { Pool } = require("pg");

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
});

let client;

async function connectToPool() {
  client = await pool.connect();
}

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
      await sendQuery(`DROP TABLE IF EXISTS ${listhubReplicaTableName}`);
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
      await sendQuery(`DROP TABLE IF EXISTS ${listhubListingsATableName}`);
      await sendQuery(`DROP TABLE IF EXISTS ${listhubListingsBTableName}`);
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
      await sendQuery(`DROP TABLE IF EXISTS ${name}`);
    }

    await sendQuery(`CREATE TABLE IF NOT EXISTS ${name}(id SERIAL PRIMARY KEY, sequence VARCHAR (30), property JSON)`);
  } catch (error) {
    console.log('createListingsTable error', error);
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

    const result = await sendQuery(query, variables);
    return result.rows[0];
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
    await client.query(updateQuery, [parseInt(result.rows[0].fulfilled_jobs_count) + 1, id])
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
        last_sync_metadata_id: lastSyncMetadata.id,
        table_name: lastSyncMetadata.table_recent,
      });
    } catch (error) {
      console.log("syncListhub error", error);
    }
  }
};

module.exports.prepareListhubTables = async (event, context) => {
  await connectToPool();

  await createListingsTables(true);
  await createReplicaTable(true);
};

/**
 * Lambda handler that invokes every 1 hour to check if ListHub has any updates.
 * This lambda handler allows us to sync our database up with the listhub database.
 */
module.exports.listhubMonitor = async (event, context) => {
  try {
    await connectToPool();
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
      await createListingsTable(lastSyncMetadata.table_recent);

      await syncListhub(metadata, lastSyncMetadata);
    }
  } catch (err) {
    console.log("listhubMonitor error", err);
  }
};

/**
 * StreamExecutor
 * Streams the range and adds all listings in that range to the database
 */
module.exports.streamExecutor = async (event, context, callback) => {
  console.log('sss', event);
  console.log("ETag " + event.range.ETag);
  console.log("Start " + event.range.start);
  console.log("End " + event.range.end);
  console.log("Table_Name " + event.table_name);
  console.log('Last sync meta data id: ', event.last_sync_metadata_id)

  const ETag = event.range.ETag;
  const start = event.range.start;
  const end = event.range.end;
  const tableName = event.table_name;
  const lastSyncMetadataId = event.last_sync_metadata_id;
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
    await connectToPool();

    await streamingPromise;
    await increaseJobCount(lastSyncMetadataId);
  } catch (error) {
    console.log('streamExecutor error', error);
    throw error;
  }
};

module.exports.monitorSync = async () => {
  try {
    await connectToPool();

    const lastSyncMetadata = await getLastSyncMetadata();
    if (lastSyncMetadata.syncing && lastSyncMetadata.jobs_count === lastSyncMetadata.fulfilled_jobs_count) {
      const query = `UPDATE ${listhubReplicaTableName} SET syncing=$1 WHERE id=$2`;
      const variables = [false, lastSyncMetadata.id];
      await sendQuery(query, variables);
    }
  } catch (error) {
    console.log("monitorSync", error);
  }
};
