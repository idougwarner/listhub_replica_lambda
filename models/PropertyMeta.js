module.exports = (sequelize, Sequelize) => {
  const PropertyMeta = sequelize.define("PropertyMeta", {
    id: {
      type: Sequelize.INTEGER,
      autoIncrement: true,
      unique: true,
      primaryKey: true,
    },
    AcceptRanges: {
      type: Sequelize.TEXT,
    },
    LastModified: {
      type: Sequelize.TEXT,
    },
    ContentLength: {
      type: Sequelize.BIGINT,
    },
    ETag: {
      type: Sequelize.TEXT,
      unique: true,
    },
    ContentType: {
      type: Sequelize.TEXT,
    },
  });

  return PropertyMeta;
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
