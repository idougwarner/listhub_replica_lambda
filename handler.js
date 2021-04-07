'use strict';

module.exports.run = async (event, context) => {
  const time = new Date();
  console.log(`Your cron function "${context.functionName}" ran at ${time} with env ${process.env.TEST_ENV}`);
};
