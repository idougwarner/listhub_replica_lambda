"use strict";
const connectToDatabase = require("../models");

// Create and Save a new Property Listing
module.exports.listCreate = async (jsonData) => {

  // Validate request
  if (!jsonData) {
    
    const result = { dataAdded: false,  listdata: null, statusCode: 500, error: "Empty Property" };

    return result;
  }

  // console.log("Listing Key " + property.listingKey);

  try {
    const { listhub_listings_a } = await connectToDatabase();

    const data = await listhub_listings_a.create(jsonData);

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

module.exports.listBulkCreate = async (jsonData) => {
  
  // Validate request
  if (!jsonData) {
    const result = { dataAdded: false, data: null, error: "No Data to Add" };

    return result;
  }

  try {
    const { listhub_listings_a } = await connectToDatabase();

    const data = await listhub_listings_a.bulkCreate(jsonData);

    if (data.length != 0) {
      const result = { dataAdded: true, data: data, error: null };

      return result;
    } else {
      const result = { dataAdded: false, error: "Problem creating Listings" };

      return result;
    }
  } catch (err) {
    console.log("Error Adding Data:"+err)
    const result = {
      dataAdded: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Could not create the Property.",
      error: err,
    };

    return result;
  }
};

// Load data in a loop

module.exports.listBulkList = async (arrayListData) => {
  
  // Validate request
  if (!arrayListData) {

    const result = { dataAdded: false, data: null, error: "No Data to Add" };

    return result;
    
  }

  try {

    // Connect once and loop throught the records as we create them
    const { listhub_listings_a } = await connectToDatabase();

    for(i=0, len=arrayListData.length; i<len; i++) {

      const data = await listhub_listings_a.create(jsonData);
      
      if (data.length != 0) {
        const result = { dataAdded: true, data: data, error: null };
  
        console.log("Added "+i+" Records")

      } else {
        const result = { dataAdded: false, error: "Problem creating Listings" };
  
        console.log(result);
      }

    }

  } catch (err) {
    console.log("Error Adding Data:"+err)
    const result = {
      dataAdded: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Could not create the Property.",
      error: err,
    };

    return result;
  }
};

// Retrieve all Properties from the database.
module.exports.listFindAll = async () => {
  try {
    const { listhub_listings_a } = await connectToDatabase();

    const data = await listhub_listings_a.findAll({ raw: true });

    if (data.length !== 0) {
      console.log("Data exists");
      console.log("Property Listing Data " + data.length);

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
      body: "Problem obtain Property Info.",
      error: err,
    };

    return result;
  }
};

module.exports.listDataExists = async () => {
  try {
    const { listhub_listings_a } = await connectToDatabase();

    const data = await listhub_listings_a.findAll({ raw: true });

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
      body: "Problem finding Property Info.",
    };

    return result;
  }
};

// Delete all Properties from the database.
module.exports.listDeleteAll = async () => {
  try {
    const { listhub_listings_a } = await connectToDatabase();

    const data = await listhub_listings_a.destroy({
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
