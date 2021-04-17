"use strict";
const { Pool, Client } = require("pg");

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT
});

// Load data in a loop

module.exports.listBulkList = async (listArray) => {

  pool.query(
    "INSERT INTO student(firstname, lastname, age, address, email)VALUES('Mary Ann', 'Wilters', 20, '74 S Westgate St', 'mroyster@royster.com')",
    (err, res) => {
      console.log(err, res);
      pool.end();
    }
  );
  
  
};
