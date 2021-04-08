const connectToDatabase = require("../models");
const TimeUtil=require("../utils/timeFunctions");

// Create and Save a new ProperyMeta
module.exports.create = (jsonData) => {
        
        // Validate request
        if (!jsonData) {
            
            const result = { dataAdded:false, error:'' };

            return result;
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

                    const result = { dataAdded:true, data:data }

                    return result;

                })
                .catch(err => {

                    const result = { dataAdded:false, error:err } 

                    return result;

                });
            }

            catch(err) 
            {
                const result = {
                    dataAdded:false,
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not create the PropertyMeta.'
                }

                return result;
            }
};

// Retrieve all Propertymeta from the database.
module.exports.findAll = (req, res) => {

        try 
        {
            const { PropertyMeta } = await connectToDatabase()
    
            PropertyMeta.findAll({ raw: true })
            .then(data => {
        
                console.log('Meta Data'+data.length);
        
                if(data.length!==0)
                {
                    console.log("Data exists")

                    const result = { dataExists:true, data:data }

                    return result;
                }
        
                else 
                {
                    const result = { dataExists:false, error: "No Data" }

                    return result;
                }
            })
            .catch(err => {
                const result = { dataExists:false, error:err } 

                return result;
        
            });
        }

        catch(err) 
        {
            const result = {
                dataExists:false,
                statusCode: 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Problem obtain PropertyMeta Info.',
                error: err
            }

            return result; 
        }
        
};

// Check if there is Metadata data
module.exports.metadataExists = () => {

        try 
        {
            const { PropertyMeta } = await connectToDatabase()
    
            PropertyMeta.findAll({ raw: true })
            .then(data => {
        
                console.log('Meta Data'+data.length);
        
                if(data.length!==0)
                {
                    console.log("Data exists")

                    const result = { dataExists:true, data:data }

                    return result;
                }
        
                else 
                {
                    const result = { dataExists:false, error: "No Data" }

                    return result;
                }
            })
            .catch(err => {

                const result = { dataExists:false,"error":err } 

                return result;
        
            });
        }

        catch(err) 
        {
            const result = {
                dataExists:false,
                statusCode: 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Problem finding PropertyMeta Info.',
                error: err
            }

            return result; 
        }
};

module.exports.ismetadataNew = (lastModified) => {

    // Check whether there is data before comparing otherwise store the new data
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
                                const result = { newUpdate: true }
                                return result;
                            }
                            else {
                                const result = { newUpdate: false }
                                return result;
                            }
                            
                        }).catch(err => {
                            console.log("Catch error after time check")
                            const result = { newUpdate: false, error: err }
                            return result;
                        });
                    })
                }            
            })
            .catch(err => {
                console.log("Checking new metadata fail");
                const result = { newUpdate: false, error: err }
                return result;
            });
        }

        catch(err) 
        {
            const result = {
                newUpdate:false,
                statusCode: 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Problem Deleting Property Info.'
            }

            return result; 
        }
}

// Delete all PropertyMetas from the database.
module.exports.deleteAll = () => {
       
            try 
            {
                const { PropertyMeta } = await connectToDatabase()
        
                PropertyMeta.destroy({
                    where: {},
                    truncate: false
                })
                .then(nums => {
                    const result = { dataDeleted:true }

                    return result;
                })
                .catch(err => {

                    const result = { dataDeleted:false, "err":err }

                    return result;

                });
            }
    
            catch(err) 
            {
                const result = {
                    dataDeleted:false,
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Problem Deleting Property Meta.'
                }
    
                return result;
            }
};