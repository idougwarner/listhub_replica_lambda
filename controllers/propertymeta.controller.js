const connectToDatabase = require("../models");
const TimeUtil=require("../utils/timeFunctions");

// Create and Save a new ProperyMeta
module.exports.create = (jsonData) => {

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
        
            try 
            {
                const { PropertyMeta } = await connectToDatabase()
        
                PropertyMeta.create(propertymeta)
                .then(data => {

                    console.log("New Meta Data is"+data);

                    resolve({dataAdded:true});

                })
                .catch(err => {

                    reject(new Error("Some error occurred while creating the PropertyMeta."));

                });
            }

            catch(err) 
            {
                reject ({
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not create the Property.'
                })
            }       
    });
};

// Retrieve all Propertymeta from the database.
module.exports.findAll = (req, res) => {

    return new Promise((resolve, reject) => {

        try 
        {
            const { PropertyMeta } = await connectToDatabase()
    
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
        }

        catch(err) 
        {
            reject ({
                statusCode: err.statusCode || 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Could not find the PropertyMeta.'
            })
        }
        
    });
};

// Check if there is Metadata data
module.exports.metadataExists = () => {

    return new Promise(function(resolve, reject) {

        try 
        {
            const { PropertyMeta } = await connectToDatabase()
    
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
        }

        catch(err) 
        {
            reject ({
                statusCode: err.statusCode || 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Could not find the PropertyMeta.'
            })
        }
    });
};

module.exports.ismetadataNew = (lastModified) => {

    // Check whether there is data before comparing otherwise store the new data
    return new Promise(function(resolve, reject) {

        try 
        {
            const { PropertyMeta } = await connectToDatabase()
    
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
        }

        catch(err) 
        {
            reject ({
                statusCode: err.statusCode || 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Could not check the PropertyMeta.'
            })
        }
      });
}

// Delete all PropertyMetas from the database.
module.exports.deleteAll = () => {

    return new Promise(function(resolve, reject) {
        
            try 
            {
                const { PropertyMeta } = await connectToDatabase()
        
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
            }
    
            catch(err) 
            {
                reject ({
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not delete the PropertyMeta.'
                })
            }
      });
};