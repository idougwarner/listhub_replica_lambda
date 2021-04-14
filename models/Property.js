module.exports = (sequelize, Sequelize) => {
  const Property = sequelize.define("PropertyFull1", {
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

  return Property;
};
