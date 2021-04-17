"use strict";
const { Pool, Client } = require("pg");

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT
});


module.exports.listBulkCreate = async (jsonData) => {
  
  // Validate request
  if (!jsonData) {
    const result = { dataAdded: false, data: null, error: "No Data to Add" };

    return result;
  }

  try {
    const { listhub_listings_a } = await connectToDatabase();

    const data = await listhub_listings_a.bulkCreate(jsonData);

    if (data.length != 0) {
      const result = { dataAdded: true, data: data, error: null };

      return result;
    } else {
      const result = { dataAdded: false, error: "Problem creating Listings" };

      return result;
    }
  } catch (err) {
    console.log("Error Adding Data:"+err)
    const result = {
      dataAdded: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Could not create the Property.",
      error: err,
    };

    return result;
  }
  
};

// Load data in a loop

module.exports.listBulkList = async (listArray) => {
  
  
};
