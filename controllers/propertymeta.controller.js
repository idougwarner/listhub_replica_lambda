const connectToDatabase = require("../models");
const TimeUtil = require("../utils/timeFunctions");

// Create and Save a new ProperyMeta
module.exports.metaCreate = async (jsonData) => {
  // Validate request
  if (!jsonData) {
    const result = { metadataAdded: false, error: "No Data", metadata:null }

    return result
  }

  /**
   * {
  /**
 * {
  "AcceptRanges": "bytes",
  "LastModified": "2019-12-18T13:55:20.000Z",
  "ContentLength": 2866064774,
  "ETag": "\"d967f79ad57127eacceb7f7e95270ff1\"",
  "ContentType": "application/octet-stream"
}
 */

  const propertymeta = {
    AcceptRanges: jsonData.AcceptRanges,
    LastModified: jsonData.LastModified,
    ContentLength: jsonData.ContentLength,
    ETag: jsonData.ETag,
    ContentType: jsonData.ContentType,
  };

  // Save PropertyMeta entry in the property table

  try {
    const { PropertyMeta } = await connectToDatabase();

    const data = await PropertyMeta.create(propertymeta);

    if (data) {
      console.log("New Meta Data is" + data);

      const result = { metadataAdded: true, metadata: data, error: null };

      return result

    } else {
      const result = {
        metadataAdded: false,
        error: "Could Not add Data",
        metadata: null
      };

      return result
    }
  } catch (err) {
    const result = {
      metadataAdded: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Could not create the PropertyMeta.",
      error: err,
      metadata:null
    };

    return result
  }
}

// Retrieve all Propertymeta from the database.
module.exports.metaFindAll = async () => {
  try {
    const { PropertyMeta } = await connectToDatabase();

    const data = await PropertyMeta.findAll({ raw: true });

    if (data.length > 0) {
      console.log("Data exists");

      const result = { dataExists: true, metadata: data, error: null };

      return result
    } else {
      const result = { dataExists: false, metadata: null, error: "No Data" };

      return result
    }
  } catch (err) {

    const result = {
      dataExists: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem obtain PropertyMeta Info.",
      error: err,
    }

    return result
  }
};

// Check if there is Metadata data
module.exports.metaDataExists = async () => {
  try {
    const { PropertyMeta } = await connectToDatabase();

    const data = await PropertyMeta.findAll({ raw: true });

    if (data.length > 0) {
      console.log("Data exists");

      const result = { dataExists: true, metadata: data, error: null };

      return result
    } else {
      const result = { dataExists: false, metadata: null, error: "No Data" };

      return result
    }
  } catch (err) {
    const result = {
      metadataExists: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem finding PropertyMeta Info.",
      error: err,
    };

    return result
  }
};

module.exports.ismetadataNew = async (lastModified) => {
  // Check whether there is data before comparing otherwise store the new data
  try {
    const { PropertyMeta } = await connectToDatabase();

    const data = await PropertyMeta.findOne({
      where: { LastModified: lastModified },
    });

    if (data.length > 0) {
      console.log("IsmetadataNew " + JSON.stringify(data));

      //console.log(propertyMeta.propertymeta.LastModified);
      let timeResult = await TimeUtil.istimeANewerthantimeB(
        lastModified,
        data.LastModified
      );

      if (timeResult.newUpdate) {
        const result = { newUpdate: true, error: null };
        return result

      } else {
        const result = { newUpdate: false, error: "No Update" };
        return result
      }
    }
  } catch (err) {
    const result = {
      newUpdate: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem Deleting Property Info.",
      error: "Problem checking meta data",
    }

    return result
  }
};

// Delete all PropertyMetas from the database.
module.exports.metaDeleteAll = async () => {
  try {
    const { PropertyMeta } = await connectToDatabase();

    const data = await PropertyMeta.destroy({
      where: {},
      truncate: false,
    });
    if (data) {
      const result = { metadataDeleted: true, error: null }

      return result
    } else {
      const result = { metadataDeleted: false, error: "Not deleted" };

      return result
    }
  } catch (err) {
    const result = {
      metadataDeleted: false,
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Problem Deleting Property Meta.",
      error: err,
    };

    return result
  }
};
