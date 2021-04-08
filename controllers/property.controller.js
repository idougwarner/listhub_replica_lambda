const connectToDatabase = require("../models");

// Create and Save a new Property Listing
module.exports.create = async (jsonData) => {

        // Validate request
        if (!jsonData) {
            
            const result = { dataAdded:false, error:'Empty Property' };

            return result;
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

                const result = { dataAdded:true, data:data }

                return result;
            })
            .catch(err => {

                const result = { dataAdded:false, error:err } 

                return result;
            });
        }
        catch(err) {

            const result = {
                dataAdded:false,
                statusCode: 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Could not create the Property.'
            }
            return result;
        }
};

module.exports.bulkCreate = async (jsonData) => {

        // Validate request
        if (!jsonData) {
            
            const result = { dataAdded:false, data:data };

            return result;
        }

        try {
            const { Property } = await connectToDatabase()

            Property.bulkCreate(jsonData)
            .then(data => {

                const result = { dataAdded:true, data:data }

                return result;
            })
            .catch(err => {
                
                const result = { dataAdded:false, error:err } 

                return result;
            });
        }
        catch(err) {
            
            const result = {
                dataAdded:false,
                statusCode: 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Could not create the Property.'
            }

            return result;   
        }
};

// Retrieve all Properties from the database.
module.exports.findAll = () => {

        try {
            const { Property } = await connectToDatabase()

            Property.findAll({ raw: true })
            .then(data => {
        
                console.log('Property Listing Data '+data.length);
        
                if(data.length!==0)
                {
                    console.log("Data exists")

                    const result = { dataExists:true, data:data }

                    return result;
                }
        
                else 
                {
                    const result = { dataExists:false, error:"No Data" }

                    return result;
                }
            })
            .catch(err => {
                const result = { dataExists:false, error:err } 

                return result;
            });
        }
        catch(err) {

            const result = {
                dataExists:false,
                statusCode: 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Problem obtain Property Info.',
                error: err
            }

            return result;  
        }

};

module.exports.propertydataExists = () => {

        try {
            const { Property } = await connectToDatabase()

            Property.findAll({ raw: true })
            .then(data => {
        
                console.log('Property Listing Data '+data.length);
        
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
        catch(err) {

            const result = {
                dataExists:false,
                statusCode: 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Problem finding Property Info.'
            }

            return result;  
        }

};

// Delete all Properties from the database.
module.exports.deleteAll = () => {

        try {
            const { Property } = await connectToDatabase()

            Property.destroy({
                where: {},
                truncate: false
              })
            .then(nums => {

                const result = { dataDeleted:true }

                return result;
            })
            .catch(err => {

                const result = { dataDeleted:false, err:err }

                return result;
            });
        }
        catch(err) {

            const result = {
                dataDeleted:false,
                statusCode: 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Problem Deleting Property Info.'
            }

            return result;   
        }
 
};