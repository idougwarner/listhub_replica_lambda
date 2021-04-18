"use strict";
const { connectToDatabase } = require("../models");

// Create and Save a new Property Listing
module.exports.list_b_Create = async (jsonData) => {

  // Validate request
  if (!jsonData) {
    
    const result = { data_b_Added: false,  list_b_data: null, status_b_Code: 500, error: "Empty Property" };

    return result;
  }

  // console.log("Listing Key " + property.listingKey);

  try {
    const { listhub_listings_b } = await connectToDatabase();

    const data = await listhub_listings_b.create(jsonData);

    if (data) {
      //console.log("New Property Data is" + data);

      const result = { data_b_Added: true, list_b_data: data, status_b_Code: 200, error_b: null };

      return result;

    } else {
      const result = { data_b_Added: false, list_b_data: null, status_b_Code: 500, error_b: "Problem Creating Data" };

      return result;
    }

  } catch (err) {
    const result = {
      data_b_Added: false,
      list_b_data: null,
      status_b_Code: 500,
      error_b: err,
      headers: { "Content-Type": "text/plain" },
      body: "Could not create the Property.",
    };

    return result;
  }
};

module.exports.list_b_BulkCreate = async (jsonData) => {
  
  // Validate request
  if (!jsonData) {
    const result = { data_b_Added: false, list_b_data: null, error_b: "No Data to Add" };

    return result;
  }

  try {
    const { listhub_listings_b } = await connectToDatabase();

    const data = await listhub_listings_b.bulkCreate(jsonData);

    if (data.length != 0) {
      const result = { data_b_Added: true, list_b_data: data, error_b: null };

      return result;
    } else {
      const result = { data_b_Added: false, error_b: "Problem creating Listings" };

      return result;
    }
  } catch (err) {
    console.log("Error Adding Data:"+err)
    const result = {
      data_b_Added: false,
      status_b_Code: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Could not create the Property.",
      error: err,
    };

    return result;
  }
};

// Load data in a loop

module.exports.list_a_BulkList = async (listArray) => {
  
  // Validate request
  if (!listArray.length) {

    const result = { data_b_Added: false, data_b: null, error: "No Data to Add" };

    return result;
    
  }

  try {

    // Connect once and loop throught the records as we create them
    const { listhub_listings_b } = await connectToDatabase();

    for(var i=0, len=listArray.length; i<len; i++) {

      const data = await listhub_listings_b.create(listArray[i]);
      
      if (data.length != 0) {
        const result = { data_b_Added: true, data_b: data, error: null };
  
        console.log("Added "+(i+1)+" Records")

      } else {
        const result = { data_b_Added: false, error: "Problem creating Listings" };
  
        console.log(result);
      }

    }

  } catch (err) {
    console.log("Error Adding Data:"+err)
    const result = {
      data_b_Added: false,
      status_b_Code: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Could not create the Property.",
      error: err,
    };

    return result;
  }
};

// Retrieve all Properties from the database.
module.exports.list_b_FindAll = async () => {
  try {
    const { listhub_listings_b } = await connectToDatabase();

    const data = await listhub_listings_b.findAll({ raw: true });

    if (data.length !== 0) {
      console.log("Data exists");
      console.log("Property Listing Data " + data.length);

      const result = { data_b_Exists: true, data_b: data };

      return result;
    } else {
      const result = { data_b_Exists: false, error_b: "No Data" };

      return result;
    }
  } catch (err) {
    const result = {
      data_b_Exists: false,
      status_b_Code: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem obtain Property Info.",
      error: err,
    };

    return result;
  }
};

module.exports.list_b_DataExists = async () => {
  try {
    const { listhub_listings_b } = await connectToDatabase();

    const data = await listhub_listings_b.findAll({ raw: true });

    if (data.length !== 0) {
      console.log("Data exists");

      const result = { data_b_Exists: true, data_b: data, error: null };

      return result;

    } else {
      const result = { data_b_Exists: false, error: "No Data", data_b: null };

      return result;
    }
  } catch (err) {
    const result = {
      data_b_Exists: false,
      status_b_Code: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem finding Property Info.",
    };

    return result;
  }
};

// Delete all Properties from the database.
module.exports.list_b_DeleteAll = async () => {
  try {
    const { listhub_listings_b } = await connectToDatabase();

    const data = await listhub_listings_b.destroy({
      where: {},
      truncate: false,
    });

    if (data.length == 0) {
      const result = { data_b_Deleted: true, error: null };

      return result;
    } else {
      const result = { data_b_Deleted: false, error: "No Data to delete" };

      return result;
    }
  } catch (err) {
    const result = {
      data_b_Deleted: false,
      status_b_Code: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem Deleting Property Info.",
      error: err,
    };

    return result;
  }
};