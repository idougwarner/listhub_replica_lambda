const Sequelize = require("sequelize");
const { Pool, Client } = require("pg");
const listhub_listings_a_model = require("./listhub_listings_a");
const listhub_listings_b_model = require("./listhub_listings_b");

const listings_meta_model = require("./listings_meta");
const listings_update_reference_model = require("./listings_update_reference");

const sequelize = new Sequelize(
  process.env.DB_NAME,
  process.env.DB_USER,
  process.env.DB_PASSWORD,
  {
    dialect: "postgres",
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
  }
);

const listhub_listings_a = listhub_listings_a_model(sequelize, Sequelize);
const listhub_listings_b = listhub_listings_b_model(sequelize, Sequelize);
const listings_meta = listings_meta_model(sequelize, Sequelize);
const listings_update_reference = listings_update_reference_model(sequelize, Sequelize);

const Models = { listhub_listings_a, listhub_listings_b, listings_meta, listings_update_reference };
const connection = {};

module.exports.connectToDatabase = async () => {
  if (connection.isConnected) {

    //console.log("=> Using existing connection.");
    return Models;

  }

  //await sequelize.sync();
  await sequelize.authenticate();

  connection.isConnected = true;

  console.log(" Created a new connection to DB");

  return Models;
};

module.exports.syncDB = async () => {
 return sequelize.sync({ force:true });
}

const pool= new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT
});

module.exports.pool = pool
