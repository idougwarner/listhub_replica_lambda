module.exports = (sequelize, Sequelize) => {
  const listhub_listings_a = sequelize.define("listhub_listings_a", {
    id: {
      type: Sequelize.INTEGER,
      autoIncrement: true,
      unique: true,
      primaryKey: true
    },
    Property: {
      type: Sequelize.JSONB
    },
    sequence: {
      type: Sequelize.BIGINT,
      unique: true
    },
  });

  return listhub_listings_a;
};
