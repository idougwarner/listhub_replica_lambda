"use strict";
const axios = require("axios");
const request = require("request");
const AWS = require("aws-sdk");
const bigInt = require("big-integer");
const ndjson = require("ndjson");
const qs = require("querystring");
const lambda = new AWS.Lambda({ region: "us-west-2" });

const { Pool } = require("pg");
const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
});
let pgClient = null;

let listhubAccessToken = null;
const listhubClientId = process.env.LISTHUB_CLIENT_ID;
const listhubClientSecret = process.env.LISTHUB_CLIENT_SECRET;
const listhubOAuth2TokenApi = process.env.LISTHUB_OAUTH2_TOKEN_API;
const listhubMetadataApi = process.env.LISTHUB_META_API;
const listhubReplicationApi = process.env.LISTHUB_REPLICATION_API;
const listhubReplicaJobsTableName = "replica_jobs";
const listhubListingsInitial = "listings_initial";

const refreshListhubToken = async () => {
  const oauth2Body = {
    grant_type: "client_credentials",
    client_id: listhubClientId,
    client_secret: listhubClientSecret,
  };

  try {
    const response = await axios.post(
      listhubOAuth2TokenApi,
      qs.stringify(oauth2Body),
      { headers: { "Content-type": "application/x-www-form-urlencoded" } }
    );
    listhubAccessToken = response.data.access_token;

    console.log('refreshListhubToken success');
  } catch (error) {
    console.log('refreshListhubToken error', error);
    listhubAccessToken = null;
  }
};

async function connectToPool() {
  pgClient = await pool.connect();
}

function releaseClient() {
  if (pgClient) {
    pgClient.release();
  }
}

const sendQuery = (query, variables) => new Promise((resolve, reject) => {
  const callback = (error, res) => {
    if (error) {
      reject(error);
    } else {
      resolve(res);
    }
  };

  if (!variables) {
    pgClient.query(query, callback);
  } else {
    pgClient.query(query, variables, callback);
  }
});

const createReplicaJobsTable = async (dropFirst = true) => {
  try {
    if (dropFirst) {
      await sendQuery(`DROP TABLE IF EXISTS ${listhubReplicaJobsTableName}`);
    }

    await sendQuery(`CREATE TABLE IF NOT EXISTS ${listhubReplicaJobsTableName}(id SERIAL PRIMARY KEY, last_modified TIMESTAMP, table_recent VARCHAR(50), table_stale VARCHAR(50), jobs_count INT, fulfilled_jobs_count INT, syncing BOOLEAN, created_at TIMESTAMP)`);

    console.log(`${listhubReplicaJobsTableName} created successfully!`);
  } catch (error) {
    console.log('createReplicaJobsTable error', error);
  }
}

const createListingsTable = async (name, dropFirst = true) => {
  try {
    if (dropFirst) {
      await sendQuery(`DROP TABLE IF EXISTS ${name}`);
    }

    await sendQuery(`CREATE TABLE IF NOT EXISTS ${name}(id SERIAL PRIMARY KEY, listing_id VARCHAR (100), address VARCHAR (255), city VARCHAR (100), state VARCHAR (2), zipcode VARCHAR (10), tsv_address tsvector, property JSON)`);
    await sendQuery(`CREATE INDEX tsv_address_idx_${Date.now()} ON ${name} USING gin(tsv_address)`);
    await sendQuery(`
      CREATE FUNCTION IF NOT EXISTS address_search_trigger() RETURNS trigger AS $$
      begin
        new.tsv_address :=
          setweight(to_tsvector(coalesce(new.address,'')), 'A') ||
          setweight(to_tsvector(coalesce(new.city,'')), 'B') ||
          setweight(to_tsvector(coalesce(new.state,'')), 'C') ||
          setweight(to_tsvector(coalesce(new.zipcode,'')), 'D');
      return new;
      end
      $$ LANGUAGE plpgsql;
      
      /* Trigger on update */
      CREATE TRIGGER tsvector_address_update BEFORE INSERT OR UPDATE
      ON ${name} FOR EACH ROW EXECUTE PROCEDURE address_search_trigger();
    `);
  } catch (error) {
    console.log('createListingsTable error', error);
  }
};

const dropListingsTable = async (tableName)  => {
  try {
    await sendQuery(`DROP TABLE IF EXISTS ${tableName}`);
  } catch (error) {
    console.log('dropListingsTable error', error);
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
    const query = `INSERT INTO ${listhubReplicaJobsTableName} (last_modified, table_recent, table_stale, jobs_count, fulfilled_jobs_count, syncing, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`;
    const variables = [lastModified, tableRecent, tableStale, jobsCount, fulfilledJobsCount, syncing, new Date()];

    const result = await sendQuery(query, variables);
    return result.rows[0];
  } catch (error) {
    console.log('addReplicaMeta error', error);
  }
};

const getLastSyncMetadata = async() => {
  try {
    const result = await sendQuery(`SELECT * from ${listhubReplicaJobsTableName} ORDER BY created_at DESC LIMIT 1`);
    return result.rows[0];
  } catch (error) {
    console.log('getLastSyncMetadata error', error);
    return null;
  }
};

const increaseJobCount = async (id) => {
  const readQuery = `SELECT fulfilled_jobs_count from ${listhubReplicaJobsTableName} where id = $1 FOR UPDATE`;
  const updateQuery = `UPDATE ${listhubReplicaJobsTableName} SET fulfilled_jobs_count = $1 WHERE id = $2 RETURNING id`;

  try {
    await pgClient.query('BEGIN');
    const result = await pgClient.query(readQuery, [id]);
    await pgClient.query(updateQuery, [parseInt(result.rows[0].fulfilled_jobs_count) + 1, id])
    await pgClient.query('COMMIT');
  } catch (error) {
    await cllient.query('ROLLBACK');
  }
};

// Get inputStream from replication request with range headers
const getMetaDataStream = () => axios({
  url: listhubMetadataApi,
  method: "get",
  headers: {
    Accept: "application/json",
    Authorization: `Bearer ${listhubAccessToken}`,
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
  console.log("ranges.length " + ranges.length);

  for (let index = 0; index < ranges.length; index++) {
    let range = ranges[index];

    console.log(`Range: ${range.start} - ${range.end}`);

    try {
      await invokeStreamExecutor({
        range: range,
        last_sync_metadata_id: lastSyncMetadata.id,
        table_name: lastSyncMetadata.table_recent,
        listhub_access_token: listhubAccessToken
      });
    } catch (error) {
      console.log("syncListhub error", error);
    }
  }
};

module.exports.prepareListhubTables = async (event, context) => {
  await connectToPool();

  await createListingsTable(listhubListingsInitial);
  await createReplicaJobsTable();

  releaseClient();
};

/**
 * Lambda handler that invokes every 1 hour to check if ListHub has any updates.
 * This lambda handler allows us to sync our database up with the listhub database.
 */
module.exports.listhubMonitor = async (event, context) => {
  await createListingsTable(`table-${Date.now()}`);

  return;
  try {
    await connectToPool();

    await refreshListhubToken();

    const response = await getMetaDataStream();
    if (response) {
      const metadata = response.data;

      let lastSyncMetadata = await getLastSyncMetadata();

      if (lastSyncMetadata && !checkIfListhubUpdated(metadata, lastSyncMetadata)) return;

      if (lastSyncMetadata && lastSyncMetadata.table_stale) {
        await dropListingsTable(lastSyncMetadata.table_stale);
      }

      const ranges = getRangesFromMetadata(metadata);

      lastSyncMetadata = await addSyncMetadata({
        lastModified: metadata.Metadata.lastmodifiedtimestamp,
        tableRecent: lastSyncMetadata ? `listhub_listings_${Date.now()}` : listhubListingsInitial,
        tableStale: lastSyncMetadata ? lastSyncMetadata.table_recent : null,
        jobsCount: ranges.length
      });

      if (lastSyncMetadata.table_recent !== listhubListingsInitial) {
        await createListingsTable(lastSyncMetadata.table_recent);
      }

      await syncListhub(metadata, lastSyncMetadata);
    }
  } catch (err) {
    console.log("listhubMonitor error", err);
  }

  releaseClient();
};

/**
 * StreamExecutor
 * Streams the range and adds all listings in that range to the database
 */
module.exports.streamExecutor = async (event, context, callback) => {
  console.log("etag: ", event.range.ETag);
  console.log("start: ", event.range.start);
  console.log("end: ", event.range.end);
  console.log("table name:  ", event.table_name);
  console.log('last sync meta data id: ', event.last_sync_metadata_id);

  listhubAccessToken = event.listhub_access_token;
  const ETag = event.range.ETag;
  const start = event.range.start;
  const end = event.range.end;
  const tableName = event.table_name;
  const lastSyncMetadataId = event.last_sync_metadata_id;

  const listingArray = [];

  // Get inputStream from replication request with range headers
  const stream = request({
    url: listhubReplicationApi,
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${listhubAccessToken}`,
      "If-Range": ETag,
      Range: "sequence=" + start + "-" + end,
    },
  });

  const streamingPromise = new Promise((resolve, reject) => {
    console.log("start time: ", new Date());

    stream.pipe(ndjson.parse())
      .on("data", (data) => {
        listingArray.push(data);
      });

    stream
      .on("complete", async () => {
        console.log(`streamed records count: ${listingArray.length}`);

        let listingsCount = listingArray.length;

        // if (listingsCount > 10) {
        //   listingsCount = 10;
        // }

        for (let index = 0; index < listingsCount; index += 1) {
          const listing = listingArray[index];
          if (!listing || !listing.sequence || !listing.Property || listing.statusCode === 412) continue;
          const query = `INSERT INTO ${tableName} (listing_id, address, city, state, zipcode, property) VALUES ($1,$2,$3,$4,$5,$6)`;
          const variables =  [listing.Property.ListingKey, listing.Property.UnparsedAddress, listing.Property.PostalCity, listing.Property.StateOrProvince, listing.Property.PostalCode, listing.Property];
          await sendQuery(query, variables);
        }

        stream.end();

        resolve();
      })
      .on("error", (err) => {
        console.log("stream error: ", err);
        stream.end();
        reject();
      });
  });

  try {
    await connectToPool();

    await streamingPromise;
  } catch (error) {
    console.log('streamExecutor error', error);
    throw error;
  }

  await increaseJobCount(lastSyncMetadataId);
  releaseClient();
};

module.exports.monitorSync = async () => {
  try {
    await connectToPool();

    const lastSyncMetadata = await getLastSyncMetadata();
    if (lastSyncMetadata.syncing && lastSyncMetadata.jobs_count === lastSyncMetadata.fulfilled_jobs_count) {
      const query = `UPDATE ${listhubReplicaJobsTableName} SET syncing=$1 WHERE id=$2`;
      const variables = [false, lastSyncMetadata.id];
      await sendQuery(query, variables);
    }
  } catch (error) {
    console.log("monitorSync", error);
  }

  releaseClient();
};
