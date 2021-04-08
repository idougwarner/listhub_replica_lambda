const connectToDatabase = require("../models");

// Create and Save a new Property Listing
module.exports.create = async (jsonData) => {

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

        try {
           const { Property } = await connectToDatabase()

           Property.create(property)
            .then(data => {
                console.log("New Property Data is"+data);

                resolve({dataAdded:true, data:data});
            })
            .catch(err => {
                reject(new Error(err));
            });
        }
        catch(err) {
            reject ({
                statusCode: err.statusCode || 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Could not create the Property.'
            })
        }            
    });
};

module.exports.bulkCreate = async (jsonData) => {

    return new Promise((resolve, reject) => {

        // Validate request
        if (!jsonData) {
            
            reject(new Error("Listing data is empty")) ;
        }

        try {
            const { Property } = await connectToDatabase()

            Property.bulkCreate(jsonData)
            .then(data => {

                resolve({dataAdded:true, data:data});
            })
            .catch(err => {
                reject(new Error(err));
            });
        }
        catch(err) {
            reject ({
                statusCode: err.statusCode || 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Could not create the Properties.'
            })   
        }
    });
};

// Retrieve all Properties from the database.
module.exports.findAll = () => {
    return new Promise((resolve, reject) => {
 
        try {
            const { Property } = await connectToDatabase()

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
        }
        catch(err) {
            reject ({
                statusCode: err.statusCode || 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Could not find the Properties.'
            })   
        }

      });
};

module.exports.propertydataExists = () => {

    return new Promise(function(resolve, reject) {

        try {
            const { Property } = await connectToDatabase()

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
        }
        catch(err) {
            reject ({
                statusCode: err.statusCode || 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Could not find Properties.'
            })  
        }
      });
};

// Delete all Properties from the database.
module.exports.deleteAll = () => {

    return new Promise(function(resolve, reject) {

        try {
            const { Property } = await connectToDatabase()

            Property.destroy({
                where: {},
                truncate: false
              })
            .then(nums => {
                resolve({ message:'Property List was deleted successfully!', deleted:true});
            })
            .catch(err => {
                reject({message:"Some error occurred while removing Property Listings.", "err":err });
            });
        }
        catch(err) {
            reject ({
                statusCode: err.statusCode || 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Could not delete the Properties.'
            })   
        }

      }); 
};