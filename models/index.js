const Sequelize = require("sequelize");
const PropertyModel = require("./Property");
const PropertyMetaModel =require("./PropertyMeta")

const sequelize = new Sequelize(
  process.env.DB_NAME,
  process.env.DB_USER,
  process.env.DB_PASSWORD,
  {
    dialect: process.env.DB_DIALECT,
    host: process.env.DB_HOST,
    port: process.env.DB_PORT
  }
)

const Property = PropertyModel(sequelize, Sequelize);
const PropertyMeta = PropertyMetaModel(sequelize, Sequelize);

const Models = { Property, PropertyMeta }
db.Sequelize = Sequelize;
db.sequelize = sequelize;

db.property = require("./Property.js")(sequelize, Sequelize);
db.propertymeta = require("./PropertyMeta.js")(sequelize, Sequelize);

module.exports = db;