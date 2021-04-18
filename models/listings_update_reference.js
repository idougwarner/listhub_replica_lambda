module.exports = (sequelize, Sequelize) => {
  const listings_update_reference = sequelize.define("listings_update_reference", {
    id: {
      type: Sequelize.INTEGER,
      autoIncrement: true,
      unique: true,
      primaryKey: true,
    },
    table_name: {
      type: Sequelize.TEXT,
    },
    last_modified: {
      type: Sequelize.TEXT,
    }
  });

  return listings_update_reference;
};

/**
 * {
  "AcceptRanges": "bytes",
  "LastModified": "2019-12-18T13:55:20.000Z",
  "ContentLength": 2866064774,
  "ETag": "\"d967f79ad57127eacceb7f7e95270ff1\"",
  "ContentType": "application/octet-stream"
}
 */
