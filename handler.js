'use strict';
const axios = require("axios");
const request = require('request');
const fs = require("fs");
const stream = require("stream");

const properties = require("./controllers/property.controller");
const propertyMeta = require("./controllers/propertymeta.controller");

const { metaURL, replicationURL, token } = require("./config/url");

class JsonLinesTransform extends stream.Transform {
  _transform(chunk, env, cb) {
    if(!this.chunks) {
      this.chunks = "";
    }

    this.chunks += chunk;

    var lines = this.chunks.split(/\n/);

    this.chunks = lines.pop();

    for(let i = 0; i < lines.length; i++) {
      this.push(lines[i]);
    }

    cb();
  }
}

// Retrieve new streamed data and store to database
const newListData = (type) => {

  return new Promise((resolve, reject) => {
      
      // Create a time object and store start time we want stream to read data for 7 minutes. 
      /* It is possible to finish reading all data in the seven minutes */
      let startTime = new Date();

      const Etag="";

      if(type.storeType==="new")
      {

          const getSize = (bytes) => {
          var marker = 1024;
          var decimal = 3; // Change as required
          var kiloBytes = marker; // One Kilobyte is 1024 bytes
          var megaBytes = marker * kiloBytes; // One MB is 1024 KB
          var gigaBytes = marker * megaBytes; // One GB is 1024 MB
          var teraBytes = marker * gigaBytes; // One TB is 1024 GB
      
          // return bytes if less than a KB
          if(bytes < kiloBytes) return { "bytes":bytes, "type": "Bytes" };
          
          // return KB if less than a MB
          else if(bytes < megaBytes) return { "bytes": ((bytes / kiloBytes).toFixed(decimal)),"type":"KB" };
          
          // return MB if less than a GB
          else if(bytes < gigaBytes) return { "bytes": (bytes / megaBytes).toFixed(decimal), "type":"MB" };
          
          // return GB if less than a TB
          else if(bytes < teraBytes) return { "bytes": (bytes / gigaBytes).toFixed(decimal), "type":"GB" };

          }
          
      

      /*
        const determineChunkRange = (startOfRange, endOfRange) => {
            
            // 1 Mib = 1,048,576 B.
            // Only byteSize chunks will be downloaded till the end where we get the remainder.
            const MiB = byteSize;
            const chunkSize = MiB * 1048576;
            const startOfRange = step === 0 ? 0 + ((chunkSize * step)) : 1 + ((chunkSize * step));
            const endOfRange = startOfRange + chunkSize;
        
            return { 'startOfRange' : startOfRange, 'endOfRange': endOfRange }
        }
      */

        // Extract new data and store Etag and sequence
        // Run get request to read data to file then read the data to the database
        const getInputStream1 = (rangeValues) => {

          // Get inputStream from replication request
          return request(
            {
                url : replicationURL,
                headers : {
                  'Accept': 'application/json',
                  'Authorization' : 'Bearer '+token,
                  'Range' : 'bytes='+rangeValues.startOfRange+'-'+rangeValues.endOfRange
                }
            });
        }
        
        console.log("Inside Replicate Data");
        
        // Get size of file in either B, KB, MB or GB
        const filetoDownloadSize = getSize(type.ContentLength);
        let chunks = "";
        let downloadedSize = 0;
        let chunkSize = 0;
        let step=0;
        let remainingDownloadSize=0;
        let startOfRange;
        let endOfRange;
                
        if(fileSize.type == 'MB') {
          // Check if filesize is between 20MB and 30MB and set chunks to be 5
          if(fileSize<=30 && fileSize>=20) {
            chunks=5; 
          } 
        }

        // If file size is in KB then there is no need to chunk
        else if(fileSize.type=='KB') {
          chunks=1;
        }

        // while downloadedfilesize!=contentLength keep downloading in specified chunks
        while(downloadedSize!=filetoDownloadSize && downloadedSize<filetoDownloadSize) 
        {
          
          var rangeValues;

          if(chunks==1) {
            chunkSize=1024;
          }

          else if(chunks==5) {
            chunkSize=parseInt(fileSize/chunks)*1048576;
          }
                   

          /* Download the first part of the data and check if remaining download size is less than the chunk size to 
          determine start and end */
          if(remainingDownloadSize==0 && step==0) // Will be true only the first time
          {
            // Set Range
            startOfRange = step === 0 ? 0 + ((chunkSize * step)) : 1 + ((chunkSize * step));
            endOfRange = startOfRange + chunkSize;

            rangeValues={'startOfRange' : startOfRange, 'endOfRange': endOfRange}
            
            /* This will be done on the finish method of inputstream meaning data has been successfully written to file
               and there was no interruption
            step=step+1;
            
            downloadedSize=endOfRange;
            */
            
          }

          // Check to see if what is remaining is bigger than the chunk size so that we set next range
          else if(downloadedSize!=0 && step!=0 && (filetoDownloadSize-downloadedSize)>chunkSize ) {

            // Set Range
            startOfRange = step === 0 ? 0 + ((chunkSize * step)) : 1 + ((chunkSize * step));
            endOfRange = startOfRange + chunkSize;
            
            rangeValues={'startOfRange' : 1+(chunkSize*step), 'endOfRange': endOfRange}
            
            /* This will be done on the finish method of inputstream meaning data has been successfully written to file
               and there was no interruption
            step=step+1;
            
            downloadedSize=endOfRange;
            */
          }

          // Check to see if what is remaining is less than chunkSize so as to determine new range limits
          else if(downloadedSize!=0 && step!=0 && (filetoDownloadSize-downloadedSize)<chunkSize) {

            // Set Range
            startOfRange = step === 0 ? 0 + ((chunkSize * step)) : 1 + ((chunkSize * step));
            endOfRange = filetoDownloadSize;
            
            rangeValues={'startOfRange' : startOfRange, 'endOfRange': endOfRange}
            
            /* This will be done on the finish method of inputstream meaning data has been successfully written to file
               and there was no interruption
            step=step+1;
            
            downloadedSize=endOfRange;
            */

          }

          // Download the data

            const inputStream = getInputStream1(rangeValues);

            const writeStream = (data) => {
                try {
                    fs.appendFileSync('./propertylisting.json', data);
                    console.log("Data appended to file.");
                } catch (err) {
                    console.log(`Error appending to './propertylisting.json'`, err);
                }
            }
        
            console.log("After create a file write stream");
    
            inputStream
              .on('response', (response) => {
                console.log("Status code "+response.statusCode);
                console.log("Etag value "+response.headers['Etag']);
                Etag=response.headers['Etag'];
                              
              })
              .pipe(new JsonLinesTransform())
              .pipe(writeStream)
              .on('finish', () => {

                // Data was successfully added therefore add step and downloadedSize and proceed to get next chunk in next loop
                step=step+1;
            
                downloadedSize=endOfRange;

                console.log('Done downloading Property Listing data!');

                let rawdata = fs.readFileSync('./propertylisting.json');
                
                // var myjson = rawdata.toString().split('}{'); 
                
                var myjson = rawdata.toString().split('}{');
        
                // Create a JSON object array for saving to database
                var mylist = '[' + myjson.join('},{') + ']';

                /* Get the last sequence value and use it to fetch data with Etag to ensure we have fetched everything
                  and no data is left
                */
                var lastRecord=JSON.parse(mylist[mylist.length()-1]);
                var metarecords={"Etag":Etag,"sequence":lastRecord.sequence};

                // Create a json file that stores Etag and Sequence value
                try {
                  
                  fs.writeFileSync('./metavalues.json', JSON.stringify(metarecords))

                } catch (err) {

                  reject(new Error(""+err))

                }
                
                const listings1 = JSON.parse(mylist);
                
                properties.bulkCreate(listings1).then(data => {
                            
                  if(data.dataAdded) {
                    resolve({ dataAdded:true, })
                  }

                }).catch(err => {

                  reject(new Error("Erro Adding Product Listing data"+err));

                });

            }) // End of Input Stream

        } // End while

      }

      // Fresh listings download
      else if(type.storeType==="newDownload") {

        properties.propertydataExists().then(data => {
        
          if(data.dataExists) {
            
            // Delete old data and put new data
            properties.deleteAll().then(data => {
  
              if(data.deleted) {

                const getSize = (bytes) => {
                  var marker = 1024;
                  var decimal = 3; // Change as required
                  var kiloBytes = marker; // One Kilobyte is 1024 bytes
                  var megaBytes = marker * kiloBytes; // One MB is 1024 KB
                  var gigaBytes = marker * megaBytes; // One GB is 1024 MB
                  var teraBytes = marker * gigaBytes; // One TB is 1024 GB
              
                  // return bytes if less than a KB
                  if(bytes < kiloBytes) return { "bytes":bytes, "type": "Bytes" };
                  
                  // return KB if less than a MB
                  else if(bytes < megaBytes) return { "bytes": ((bytes / kiloBytes).toFixed(decimal)),"type":"KB" };
                  
                  // return MB if less than a GB
                  else if(bytes < gigaBytes) return { "bytes": (bytes / megaBytes).toFixed(decimal), "type":"MB" };
                  
                  // return GB if less than a TB
                  else if(bytes < teraBytes) return { "bytes": (bytes / gigaBytes).toFixed(decimal), "type":"GB" };
        
                  }
                  
              
        
              /*
                const determineChunkRange = (startOfRange, endOfRange) => {
                    
                    // 1 Mib = 1,048,576 B.
                    // Only byteSize chunks will be downloaded till the end where we get the remainder.
                    const MiB = byteSize;
                    const chunkSize = MiB * 1048576;
                    const startOfRange = step === 0 ? 0 + ((chunkSize * step)) : 1 + ((chunkSize * step));
                    const endOfRange = startOfRange + chunkSize;
                
                    return { 'startOfRange' : startOfRange, 'endOfRange': endOfRange }
                }
              */
        
                // Extract new data and store Etag and sequence
                // Run get request to read data to file then read the data to the database
                const getInputStream1 = (rangeValues) => {
        
                  // Get inputStream from replication request
                  return request(
                    {
                        url : replicationURL,
                        headers : {
                          'Accept': 'application/json',
                          'Authorization' : 'Bearer '+token,
                          'Range' : 'bytes='+rangeValues.startOfRange+'-'+rangeValues.endOfRange
                        }
                    });
                }
                
                console.log("Inside Replicate Data");
                
                // Get size of file in either B, KB, MB or GB
                const filetoDownloadSize = getSize(type.ContentLength);
                const chunks = "";
                const downloadedSize = 0;
                const chunkSize = 0;
                const step=0;
                const remainingDownloadSize=0;
                const startOfRange=0;
                const endOfRange=0;
                        
                if(fileSize.type == 'MB') {
                  // Check if between 20 and 30 and set chunks to be 5
                  if(fileSize<=30 && fileSize>=20) {
                    chunks=5; 
                  } 
                }
        
                else if(fileSize.type=='KB') {
                  chunks=1;
                }
        
                // while downloadedfilesize!=contentLength keep downloading in specified chunks
                while(downloadedSize!=filetoDownloadSize && downloadedSize<filetoDownloadSize) 
                {
                  // Set the chunk size
        
                  var rangeValues;
        
                  if(chunks==1) {
                    chunkSize=1024;
                  }
        
                  else if(chunks==5) {
                    chunkSize=parseInt(fileSize/chunks)*1048576;
                  }
                           
        
                  /* Download the first part of the data and check if remaining download size is less than the chunk size to 
                  determine start and end */
                  if(remainingDownloadSize==0 && step==0) // Will be true only the first time
                  {
                    // Set Range
                    startOfRange = step === 0 ? 0 + ((chunkSize * step)) : 1 + ((chunkSize * step));
                    endOfRange = startOfRange + chunkSize;
        
                    rangeValues={'startOfRange' : startOfRange, 'endOfRange': endOfRange}
                    
                    /* This will be done on the finish method of inputstream meaning data has been successfully written to file
                       and there was no interruption
                    step=step+1;
                    
                    downloadedSize=endOfRange;
                    */
                    
                  }
        
                  // Check to see if what is remaining is bigger than the chunk size so that we set next range
                  else if(downloadedSize!=0 && step!=0 && (filetoDownloadSize-downloadedSize)>chunkSize ) {
        
                    // Set Range
                    startOfRange = step === 0 ? 0 + ((chunkSize * step)) : 1 + ((chunkSize * step));
                    endOfRange = startOfRange + chunkSize;
                    
                    rangeValues={'startOfRange' : 1+(chunkSize*step), 'endOfRange': endOfRange}
                    
                    /* This will be done on the finish method of inputstream meaning data has been successfully written to file
                       and there was no interruption
                    step=step+1;
                    
                    downloadedSize=endOfRange;
                    */
                  }
        
                  // Check to see if what is remaining is less than chunkSize so as to determine new range limits
                  else if(downloadedSize!=0 && step!=0 && (filetoDownloadSize-downloadedSize)<chunkSize) {
        
                    // Set Range
                    startOfRange = step === 0 ? 0 + ((chunkSize * step)) : 1 + ((chunkSize * step));
                    endOfRange = filetoDownloadSize;
                    
                    rangeValues={'startOfRange' : startOfRange, 'endOfRange': endOfRange}
                    
                    /* This will be done on the finish method of inputstream meaning data has been successfully written to file
                       and there was no interruption
                    step=step+1;
                    
                    downloadedSize=endOfRange;
                    */
        
                  }
        
                  // Download the data
        
                    const inputStream = getInputStream1(rangeValues);
        
                    const writeStream = (data) => {
                        try {
                            fs.appendFileSync('./propertylisting.json', data);
                            console.log("Data appended to file.");
                        } catch (err) {
                            console.log(`Error appending to './propertylisting.json'`, err);
                        }
                    }
                
                    console.log("After create a file write stream");
            
                    inputStream
                      .on('response', (response) => {
                        console.log("Status code "+response.statusCode);
                        console.log("Etag value "+response.headers['Etag']);
                        Etag=response.headers['Etag'];
                                      
                      })
                      .pipe(new JsonLinesTransform())
                      .pipe(writeStream)
                      .on('finish', () => {
        
                        // Data was successfully added therefore add step and downloadedSize and proceed to get next chunk in next loop
                        step=step+1;
                    
                        downloadedSize=endOfRange;
        
                        console.log('Done downloading Property Listing data!');
        
                        let rawdata = fs.readFileSync('./propertylisting.json');
                        
                        // var myjson = rawdata.toString().split('}{'); 
                        
                        var myjson = rawdata.toString().split('}{');
                
                        // Create a JSON object array for saving to database
                        var mylist = '[' + myjson.join('},{') + ']';
        
                        /* Get the last sequence value and use it to fetch data with Etag to ensure we have fetched everything
                          and no data is left
                        */
                        var lastRecord=JSON.parse(mylist[mylist.length()-1]);
                        var metarecords={"Etag":Etag,"sequence":lastRecord.sequence};
        
                        // Create a json file that stores Etag and Sequence value
                        try {
                          
                          fs.writeFileSync('./metavalues.json', JSON.stringify(metarecords))
        
                        } catch (err) {
        
                          reject(new Error(""+err))
        
                        }
                        
                        const listings1 = JSON.parse(mylist);
                        
                        properties.bulkCreate(listings1).then(data => {
                                    
                          if(data.dataAdded) {
                            resolve({ dataAdded:true, })
                          }
        
                        }).catch(err => {
        
                          reject(new Error("Erro Adding Product Listing data"+err));
        
                        });
        
                    }) // End of Input Stream
        
                } // End while
              } // Data deleted successfully
  
            }).catch(err=>{
  
              reject(new Error("Problem deleting old Property listing data"+err));
  
            })
          }
  
          else {
  
            console.log("Inside else fresh download data ******************* ");
  
            // Get the Data
  
            const getInputStream1 = () => {
  
              // Get inputStream from replication request with range headers
              return request(
                {
                    url : replicationURL,
  
                    headers : {
                      'Accept': 'application/json',
                      'Authorization' : 'Bearer '+token
                    }
                });
  
            }
            
            console.log("Inside Replicate Data");
            
            const inputStream = getInputStream1();
            const writeStream = fs.createWriteStream('./propertylisting.json');
            
            console.log("After create a file write stream");
        
            inputStream
                .on('response', (response) => {
  
                  // Get 
  
                  console.log(response.statusCode); // 200
  
                  // console.log(response.headers['content-length']);

                  total_bytes = parseInt(response.headers['content-length']);
  
                })
                .on('data', (chunk) => {
                  
                  const showDownloadingProgress = () => {
                    
                    var percentage = ((received * 100) / total).toFixed(2);
                    console.log(percentage + "% | " + received + " bytes downloaded out of " + total + " bytes.");

                  }

                  received_bytes += chunk.length;

                  showDownloadingProgress(received_bytes, total_bytes);

                })
                .pipe(new JsonLinesTransform())
                .pipe(writeStream)
                .on('finish', () => {
  
                  console.log('Done downloading Property Listing data!');
                  
                  // create a readjson
                  const jsonfile = fs.createReadStream('./propertylisting.json');
  
                  let rawdata = fs.readFileSync('./propertylisting.json');
  
                  // console.log("RAW Data "+rawdata);
                  
                  var myjson = rawdata.toString().split("}{");
  
                  // Create a JSON object array
                  // [myjson.join('},{')]
                  var mylist = '['+myjson.join('},{')+']';
  
                  const listings1 = JSON.parse(mylist);
  
                  properties.bulkCreate(listings1).then(data => {
                              
                    if(data.dataAdded) {
                      resolve({ dataAdded:true, })
                    }
  
                  }).catch(err => {
  
                    reject(new Error("Erro Adding Product Listing data"+err));                            
  
                  });
                    
                })    
          }
  
        }).catch( err=> {
  
          reject(new Error("Problem checking if Property listing data exists"+err));
        })
      }     
               
  });
}

module.exports.run = async (event, context) => {

  const time = new Date();
  
  const db = {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    name: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD
  };

  console.log(`Your cron function "${context.functionName}" ran at ${time} with db ${JSON.stringify(db, null, 2)}`);
};

module.exports.fetchListingsData = async (event, context) => {
  // Call Metadata URL to get necessary data
  axios.get(metaURL, {
    headers: {
      'Authorization' : 'Bearer '+token
    }
    })
      .then(response => {                       
        
        console.log(response.data);           
        
        // this is the response body from Listhub (https://api.listhub.com/replication/metadata)
    
        // Check if metadata is newer than what is stored
    
        console.log("Last Modified is "+response.data.LastModified);
        console.log("Content Length: "+response.data.ContentLength);
        console.log("Etag Value: "+response.data.Etag);
      
        // CHECK IF PRODUCT LISTING DATA EXISTS AND POPULATE
        properties.propertydataExists().then(data => {
          
          if(data.dataExists!==true) {
    
            // Pass value to replication function
            // This replicate function is populating data for the first time
    
            newListData({"storeType":"new", "contentLength":response.data.ContentLength}).then(data => {
    
              if(data.dataAdded==true){
                console.log("Product List Data Added");
              }
    
              else {
                console.log("Problem adding data");
              }
    
            }).catch(err=>{
              
              console.log("Error replicating data "+err);
    
            });
    
          }
    
          // If it does exist do nothing unless the metadata suggests that there is an update on listings
    
        }).catch(err => {
          console.log("Error checking Property Listing Data"+err);
        })
    
        // CHECK WHETHER PROPERTY METADATA EXISTS AND IF NOT CREATE NEW METADATA
        propertyMeta.metadataExists().then((metadata) => {
    
          console.log("Exists data promise:"+data);
    
          // If metadata does not exist then store to database
          if(metadata.dataExists!==true) {
    
            // Store the new Metadata
            propertyMeta.create(response.data).then(data => {
              
              // Check if meta has been stored
              if(data.dataAdded) {
                console.log("New metadata has been created");
              }
    
            }).catch(err=> {
              console.log("Error creating new MetaData"+err);
            });
          }
    
          // Check if Property listing exists and populate if not
          properties.propertydataExists().then(data => {
            
            if(data.dataExists!==true) {
              // Call Replicate data to populate new data
              newListData({"storeType":"new", "contentLength":response.data.ContentLength}).then(data => {
    
                if(data.dataAdded){
                  console.log("New Property Listings downloaded");
                }
    
              }).catch(err=>{
              
              console.log("Error replicating data"+err)
    
              });              
            }
    
            else {
              console.log("Product listing data exists");
            }
    
          }).catch(err => {
            console.log('Error No Product list data'+err);
          })
                      
          // Compare stored meta data and new meta data coming in from Metadata URL to see if we have new listings
          propertyMeta.ismetadataNew(response.data.LastModified).then((data) => {
            
            console.log("New listings ready for download: "+data.newUpdate);
    
            if(data.newUpdate===true) {
              
              // If new metadata detected delete old metadata and save new metadata and call replicationData to download new listings
              propertyMeta.deleteAll().then(data => {
    
                if(data.deleted)
                {
                  // Store the new Metadata
                  propertyMeta.create(response.data).then(data => {
                    
                    console.log("New Metadata"+JSON.stringify(data));
    
                    // Request new Product Listing data to property listing
                    newListData({ "storeType":"newDownload", "contentLength":response.data.ContentLength }).then(data => {
    
                      if(data.dataAdded){
                        console.log("New Property Listings downloaded");
                      }
    
                    }).catch(err=>{
                    
                    console.log("Error replicating data"+err)
    
                    });
    
                  }).catch(err=> {
                    console.log(err);
                  });
                }
    
              }).catch(err =>{
                console.log('Error deleting old metadata'+err);
              })
            }
    
            else {
              // Do nothing to existing listings
            }
            
          }).catch(err => {
            console.log("No new listing data promise Error is"+err);
          });
    
        }).catch(err => {
    
          console.log("Problem checking metadata"+err);
    
        });
    
      })
      .catch(error => {
    
        console.log("This is my error: "+error);
    
      });  
}

