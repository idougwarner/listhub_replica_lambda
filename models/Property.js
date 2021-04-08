module.exports = (sequelize, Sequelize) => {
    const Property = sequelize.define("Property", {
      id: {
        type: Sequelize.INTEGER, 
        autoIncrement: true,
        unique: true,
        primaryKey: true
      },
      ListingKey: {
        type: Sequelize.TEXT
      },
      sequence: {
        type: Sequelize.BIGINT,
        unique: true
      }
    });
  
    return Property;
  };