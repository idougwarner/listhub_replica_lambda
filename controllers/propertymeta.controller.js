const db = require("../models");
const TimeUtil=require("../utils/timeFunctions");

const PropertyMeta = db.propertymeta;

// Create and Save a new ProperyMeta
const create = (jsonData) => {

        return new Promise((resolve, reject) => {
            // Validate request
        if (!jsonData) {
            
            reject(new Error("Listing Meta data is empty")) ;
        }

        // Create a Property Listing
        const propertymeta = {
            propertymeta: jsonData
        };

        // Save PropertyMeta entry in the property table
        PropertyMeta.create(propertymeta)
            .then(data => {

                console.log("New Data is"+data);

                resolve({dataAdded:true});

            })
            .catch(err => {

                reject(new Error("Some error occurred while creating the PropertyMeta."));

            });
            
        });
};

// Retrieve all Propertymeta from the database.
const findAll = (req, res) => {

    return new Promise((resolve, reject) => {
        PropertyMeta.findAll({ raw: true })
        .then(data => {
    
            console.log('Meta Data'+data.length);
    
            if(data.length!==0)
            {
                console.log("Data exists")
                resolve({dataExists:true,data:data});
            }
    
            else 
            {
                resolve({dataExists:false});
            }
        })
        .catch(err => {
          reject(new Error("Problem accessing data error is"));
    
        });
      });
};

// Find a single Property with an id
const findOne = (req, res) => {
  
};

// Check if there is Metadata

const metadataExists = () => {

    return new Promise(function(resolve, reject) {
        PropertyMeta.findAll({ raw: true })
        .then(data => {
    
            console.log('Meta Data'+data.length);
    
            if(data.length!==0)
            {
                console.log("Meta Data exists")
                resolve({dataExists:true, data: data.length});
            }
    
            else 
            {
                resolve({ dataExists:false });
                
            }
        })
        .catch(err => {
            reject(new Error('Problem checking Metadata exists!'+err));
    
        });
      });
};


const ismetadataNew = (lastModified) => {

    // Check whether there is data before comparing otherwise store the new data
    return new Promise(function(resolve, reject) {

        PropertyMeta.findAll({ })
        .then(data => {

            console.log('IsmetadataNew '+JSON.stringify(data));
            if(data.length>0)
            {
                data.map((propertyMeta) => {
                    //console.log(propertyMeta.propertymeta.LastModified);
                    TimeUtil.istimeANewerthantimeB(lastModified,propertyMeta.propertymeta.LastModified).then(data => {
                        if(data.newUpdate)
                        {
                            resolve({"newUpdate":true});
                        }
                        else {
                            resolve({"newUpdate":false});
                        }
                        
                    }).catch(err => {
                        console.log("Catch error after time check")
                        reject(new Error(err))
                    });
                })
            }            
        })
        .catch(err => {
            console.log("Checking new metadata fail");
         reject(new Error(err));

        });
      });
}

// Delete all PropertyMetas from the database.
const deleteAll = () => {

    return new Promise(function(resolve, reject) {

        PropertyMeta.destroy({
            where: {},
            truncate: false
          })
            .then(nums => {
              resolve({ message:'Meta was deleted successfully!', deleted:true});
            })
            .catch(err => {
              reject({message:"Some error occurred while removing Property Meta." });
            });
      });
};

module.exports.create=create;
module.exports.findAll=findAll;
module.exports.findOne=findOne;
module.exports.metadataExists=metadataExists;
module.exports.ismetadataNew=ismetadataNew;
module.exports.deleteAll=deleteAll;