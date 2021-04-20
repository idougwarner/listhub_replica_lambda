const { connectToDatabase, pool} = require("../models");
const TimeUtil = require("../utils/timeFunctions");
const tbl_listings_meta="listings_meta";

// Create and Save a new ProperyMeta
module.exports.metaCreate = async (jsonData) => {
  // Validate request
  if (!jsonData) {
    const result = { metadataAdded: false, error: "No Data", metadata: null };

    return result;
  }

  const propertymeta = {
    AcceptRanges: jsonData.AcceptRanges,
    LastModified: jsonData.LastModified,
    ContentLength: jsonData.ContentLength,
    ETag: jsonData.ETag,
    ContentType: jsonData.ContentType,
  };

  // Save PropertyMeta entry in the property table

  try {
    const { listings_meta } = await connectToDatabase();

    const data = await listings_meta.create(propertymeta);

    if (data) {
      console.log("New Meta Data is" + data);

      const result = { metadataAdded: true, metadata: data, error: null };

      return result;
    } else {
      const result = {
        metadataAdded: false,
        error: "Could Not add Data",
        metadata: null,
      };

      return result;
    }
  } catch (err) {
    const result = {
      metadataAdded: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Could not create the PropertyMeta.",
      error: err,
      metadata: null,
    };

    return result;
  }
};


module.exports.create_new_meta_data = async (data) => {

  try {
    await pool.connect((err, client, done) => {

      client.query(
        `INSERT INTO ${tbl_listings_meta} (id, last_modifed, content_length, etag, content_type) VALUES (DEFAULT, $1,$2,$3,$4) RETURNING id`, 
        [data.LastModified, data.ContentLength, data.ETag, data.ContentType], (err, result) => {
            if (err) {
                console.log(err);

                const result = {
                  metadataAdded: false,
                  error: "Could Not add Data",
                  metadata: null,
                };
          
                return result;
            } else {
                console.log('row inserted with id: ' + result.rows[0].id);
                
                const result = { metadataAdded: true, metadata: data, error: null };

                return result;
            }

      })

    })
  }
  catch(err) {
    const result = {
      metadataAdded: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Could not create the PropertyMeta.",
      error: err,
      metadata: null,
    };

    return result;   
  }

}

// Retrieve all Propertymeta from the database.
module.exports.metaFindAll = async () => {
  try {
    const { listings_meta } = await connectToDatabase();

    const data = await listings_meta.findAll({ raw: true });

    if (data.length > 0) {
      console.log("Data exists");

      const result = { dataExists: true, metadata: data, error: null };

      return result;
    } else {
      const result = { dataExists: false, metadata: null, error: "No Data" };

      return result;
    }
  } catch (err) {
    const result = {
      dataExists: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem obtain PropertyMeta Info.",
      error: err,
    };

    return result;
  }
};

// Check if there is Metadata data
module.exports.metaDataExists = async () => {
  try {
    const { listings_meta } = await connectToDatabase();

    const data = await listings_meta.findAll({ raw: true });

    if (data.length > 0) {
      console.log("Data exists");

      const result = { dataExists: true, metadata: data, error: null };

      return result;
    } else {
      const result = { dataExists: false, metadata: null, error: "No Data" };

      return result;
    }
  } catch (err) {
    const result = {
      metadataExists: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem finding PropertyMeta Info.",
      error: err,
    };

    return result;
  }
};

module.exports.meta_data_exist = async () => {

  const result = { metadataExists: false, metadata: null, error: null, statusCode: null,  headers: null,
    body: "" };

  try {  

    await pool.connect((err, client, done) => {

      client.query(`SELECT * from ${tbl_listings_meta}`, (err, res) => {
        
        if(res.rows) {

          console.log("Meta Data does not exist")
  
          result.metadataExists = true 
          result.metadata = data
          result.error = null

          return (result);
        } else {

          console.log("Meta Data does not exist")

          result.metadataExists = false
          result.metadata = null
          result.error = "No meta data"
    
          return (result);
        }

      });
    })
  }
  catch(err) {
        result.metadataExists = false
      result.statusCode = 500
      result.headers = { "Content-Type": "text/plain" }
      result.body = "Problem finding PropertyMeta Info."
      result.error = err

    return (result);   
  }
};

module.exports.ismetadataNew = async (lastModified) => {

  // Check whether there is data before comparing otherwise store the new data
  try {

    const { listings_meta } = await connectToDatabase();

    const data = await PropertyMeta.findOne({
      where: { LastModified: lastModified },
    });

    console.log("Metadata Result " + JSON.stringify(data));

    if (data) {

      console.log("Is Metadata New " + JSON.stringify(data));

      //console.log(propertyMeta.propertymeta.LastModified);
      let timeResult = await TimeUtil.istimeANewerthantimeB(
        lastModified,
        data.LastModified
      );

      console.log("TimeResult"+JSON.stringify(timeResult))

      if (timeResult.newUpdate) {

        const result = { newUpdate: true, error: null };
        
        return result;

      } else {
        
        const result = { newUpdate: false, error: "No Update" };
        return result;

      }
    }

    else {
      console.log("No New MetaData")
    }

  } catch (err) {

    const result = {
      newUpdate: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem Deleting Property Info.",
      error: err,
    };

    return result;

  }

};

// Delete all PropertyMetas from the database.
module.exports.metaDeleteAll = async () => {
  try {
    const { listings_meta } = await connectToDatabase();

    const data = await listings_meta.destroy({
      where: {},
      truncate: false,
    });
    if (data) {
      const result = { metadataDeleted: true, error: null };

      return result;

    } else {
      const result = { metadataDeleted: false, error: "Not deleted" };

      return result;

    }
  } catch (err) {
    
    const result = {
      metadataDeleted: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem Deleting Property Meta.",
      error: err,
    };

    return result;

  }
};

module.exports.is_meta_data_new = async (newtime) => {

  try {
    await pool.connect((err, client, done) => {

      client.query(`SELECT * from ${tbl_listings_meta}`, (err, res) => {
        
        if(res.row[0]) {
  
          var storedTime = res.row[0].last_modified
          
          let timeResult = TimeUtil.istimeANewerthantimeB(
            newtime,
            storedTime
          );
    
          console.log("TimeResult"+JSON.stringify(timeResult))
    
          if (timeResult.newUpdate) {
    
            const result = { newUpdate: true, error: null };
  
            client.end()
            
            return result;
            
    
          } else {
            
            const result = { newUpdate: false, error: "No Update" };
  
            client.end()

            return result;
    
          }
        }

      });
    })
  }
  catch(err) {
    const result = {
      newUpdate: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem Deleting Property Info.",
      error: err,
    };

    return result;    
  }

  /*client.query(
    'INSERT INTO "listhub_listings_as" ("sequence","Property", "createdAt", "updatedAt") VALUES ($1,$2,$3,$4) RETURNING id', 
    [data.sequence, data.Property, time, time], (err, result) => {
        if (err) {
            console.log(err);
        } else {
            console.log('row inserted with id: ' + result.rows[0].id);
        }

        count++;

        console.log('count = ' + count);
        
        if (count == listArray.length) {
            console.log('Client will end now!!!');
            
        }
    })*/

}