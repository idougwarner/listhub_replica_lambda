const { connectToDatabase, pool} = require("../models");
const TimeUtil = require("../utils/timeFunctions");
const tbl_listings_update_reference="listings_update_reference";


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

module.exports.table_to_save_listings = async () => {

      var list_a_table = "listhub_listings_a"
      var list_b_table = "listhub_listings_b"
      var list_a_time_modified, list_b_time_modified;

      const client = await pool().connect()
        
      // Get list_a_time_modifed
      await client.query(
        `SELECT * FROM ${listings_update_reference_table} WHERE table_name = $1)`,
        [list_a_table],  (err, result) => {
          list_a_time_modified = result.rows[0].last_modified;
        }
      );

      await client.query(
        `SELECT * FROM ${listings_update_reference_table} WHERE table_name = $1)`,
        [list_b_table],
        function(err, result) {
          list_b_time_modified = result.rows[0].last_modified;
        }
      );

      if(Date.parse(list_a_time_modified) > Date.parse(list_b_time_modified)){
        
        // Keep list_a and overwrite list_b

        return ({table_to_save:list_b_table})
        
      }
      else {

        // Keep list_b and overwrite list_a

        return ({table_to_save:list_a_table})

      } // End else
}

// Update table status
module.exports.update_table_status = async (table_name, live_status) => {
  
  try {
    const client = await pool().connect()
        
    // Get list_a_time_modifed
    const result = await client.query(
      "UPDATE listings_update_reference SET live_status = $1 WHERE table_name = $2",
      [live_status, table_name]
      );
      return ({updated:true, data: result.rows[0]})

  } catch(err) {

    console.log("Update status error"+err)
    return ({updated:false, data: result.rows[0], error:err})    

  }

}