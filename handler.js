'use strict';

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
