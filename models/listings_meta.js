module.exports = (sequelize, Sequelize) => {
  const listings_meta = sequelize.define("listings_meta", {
    id: {
      type: Sequelize.INTEGER,
      autoIncrement: true,
      unique: true,
      primaryKey: true,
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

  return listings_meta;
};
