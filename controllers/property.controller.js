const db = require("../models");
const Property = db.property;

// Create and Save a new Property Listing
const create = (jsonData) => {

        return new Promise((resolve, reject) => {

        // Validate request
        if (!jsonData) {
            
            reject(new Error("Listing data is empty")) ;
        }

        // Create a Property Listing
        const property = {
            listingKey: jsonData.ListingKey,
            sequence: jsonData.sequence
        };

        console.log("Listing Key "+property.listingKey);

        // Save Property in the database
                Property.create(property)
                .then(data => {
                    console.log("New Property Data is"+data);

                    resolve({dataAdded:true, data:data});
                })
                .catch(err => {
                    reject(new Error(err));
                });
        });
};

const bulkCreate = (jsonData) => {

    return new Promise((resolve, reject) => {

    // Validate request
    if (!jsonData) {
        
        reject(new Error("Listing data is empty")) ;
    }

    //console.log("Listing Key "+jsonData);

    // Save Property in the database
            Property.bulkCreate(jsonData)
            .then(data => {
                
                //console.log("Bulk List created "+data);

                resolve({dataAdded:true, data:data});
            })
            .catch(err => {
                reject(new Error(err));
            });
    });
};

// Retrieve all Properties from the database.
const findAll = () => {
    return new Promise((resolve, reject) => {
        Property.findAll({ raw: true })
        .then(data => {
    
            console.log('Property Listing Data '+data.length);
    
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

// Update a Property by the id in the request
const update = (req, res) => {
  
};

const propertydataExists = () => {

    return new Promise(function(resolve, reject) {
        Property.findAll({ raw: true })
        .then(data => {
    
            console.log('Property Data Exists'+data.length);
    
            if(data.length!==0)
            {
                console.log("Property Data exists")
                resolve({dataExists:true, data:data});
            }
    
            else 
            {
                resolve({dataExists:false, data:data});
            }
        })
        .catch(err => {
            reject(new Error('Problem with database check later!'));
    
        });
      });
};

// Delete all Properties from the database.
const deleteAll = () => {

    return new Promise(function(resolve, reject) {

        Property.destroy({
            where: {},
            truncate: false
          })
            .then(nums => {
              resolve({ message:'Property List was deleted successfully!', deleted:true});
            })
            .catch(err => {
              reject({message:"Some error occurred while removing Property Listings." });
            });
      }); 
};

module.exports.create=create;
module.exports.bulkCreate=bulkCreate;
module.exports.findAll=findAll;
module.exports.findOne=findOne;
module.exports.propertydataExists=propertydataExists;
module.exports.update=update;
module.exports.deleteAll=deleteAll;