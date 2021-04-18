module.exports = (sequelize, Sequelize) => {
  const listhub_listings_a = sequelize.define("listhub_listings_a", {
    Property: {
      type: Sequelize.JSONB
    },
    sequence: {
      type: Sequelize.TEXT,
      unique: true
    },
  });

  return listhub_listings_a;
};
