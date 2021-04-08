module.exports = (sequelize, Sequelize) => {
  const PropertyMeta = sequelize.define("PropertyMeta", {
    id: {
      type: Sequelize.INTEGER,
      autoIncrement: true,
      unique: true,
      primaryKey: true,
    },
    propertymeta: {
      type: Sequelize.JSONB,
    },
  });

  return PropertyMeta;
};
