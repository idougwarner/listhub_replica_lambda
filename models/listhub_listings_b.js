module.exports = (sequelize, Sequelize) => {
  const Listhub_listings_b = sequelize.define("listhub_listings_b", {
    id: {
      type: Sequelize.INTEGER,
      autoIncrement: true,
      unique: true,
      primaryKey: true
    },
    Property: {
      type: Sequelize.JSON
    },
    sequence: {
      type: Sequelize.BIGINT,
      unique: true
    },
  });

  return Listhub_listings_b;
};
