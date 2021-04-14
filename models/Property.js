module.exports = (sequelize, Sequelize) => {
  const Property = sequelize.define("PropertyFull2", {
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

  return Property;
};
