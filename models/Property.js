module.exports = (sequelize, Sequelize) => {
  const Property = sequelize.define("Property", {
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
