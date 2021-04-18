const { connectToDatabase } = require("../models");

// Create and Save a new Property Listing
module.exports.listReferenceCreate = async (jsonData) => {

  // Validate request
  if (!jsonData) {
    
    const result = { dataAdded: false,  listdata: null, statusCode: 500, error: "Empty Property" };

    return result;
  }

  // console.log("Listing Key " + property.listingKey);

  try {
    const { listings_update_reference } = await connectToDatabase();

    const data = await listings_update_reference.create(jsonData);

    if (data) {
      //console.log("New Property Data is" + data);

      const result = { dataAdded: true, listdata: data, statusCode: 200, error: null };

      return result;

    } else {
      const result = { dataAdded: false, listdata: null, statusCode: 500, error: "Problem Creating Data" };

      return result;
    }

  } catch (err) {
    const result = {
      dataAdded: false,
      listdata: null,
      statusCode: 500,
      error: err,
      headers: { "Content-Type": "text/plain" },
      body: "Could not create the Property.",
    };

    return result;
  }
};

module.exports.listReferenceBulkCreate = async (jsonData) => {
  
  // Validate request
  if (!jsonData) {
    const result = { dataAdded: false, data: null, error: "No Data to Add" };

    return result;
  }

  try {
    const { listings_update_reference } = await connectToDatabase();

    const data = await listings_update_reference.bulkCreate(jsonData);

    if (data.length != 0) {
      const result = { dataAdded: true, data: data, error: null };

      return result;
    } else {
      const result = { dataAdded: false, error: "Problem creating reference" };

      return result;
    }
  } catch (err) {
    console.log("Error Adding Data:"+err)
    const result = {
      dataAdded: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Could not create the Reference.",
      error: err,
    };

    return result;
  }
};

// Retrieve all Properties from the database.
module.exports.listReferenceFindAll = async () => {
  try {
    const { listings_update_reference } = await connectToDatabase();

    const data = await listings_update_reference.findAll({ raw: true });

    if (data.length !== 0) {
      console.log("Data exists");
      console.log("Listings reference Data " + data.length);

      const result = { dataExists: true, data: data };

      return result;
    } else {
      const result = { dataExists: false, error: "No Data" };

      return result;
    }
  } catch (err) {
    const result = {
      dataExists: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem obtain Reference Info.",
      error: err,
    };

    return result;
  }
};

module.exports.listReferenceExists = async () => {
  try {
    const { listings_update_reference } = await connectToDatabase();

    const data = await listings_update_reference.findAll({ raw: true });

    if (data.length !== 0) {
      console.log("Data exists");

      const result = { dataExists: true, data: data, error: null };

      return result;
    } else {
      const result = { dataExists: false, error: "No Data", data: null };

      return result;
    }
  } catch (err) {
    const result = {
      dataExists: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem finding Reference Info.",
    };

    return result;
  }
};

// Delete all Reference from the database.
module.exports.listReferenceDeleteAll = async () => {
  try {
    const { listings_update_reference } = await connectToDatabase();

    const data = await listings_update_reference.destroy({
      where: {},
      truncate: false,
    });

    if (data.length == 0) {
      const result = { dataDeleted: true, error: null };

      return result;
    } else {
      const result = { dataDeleted: false, error: "No Data to delete" };

      return result;
    }
  } catch (err) {
    const result = {
      dataDeleted: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem Deleting Property Info.",
      error: err,
    };

    return result;
  }
};