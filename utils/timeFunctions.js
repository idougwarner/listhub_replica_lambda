// Newer time A should be more than older time B
module.exports.istimeANewerthantimeB = (newTime, storedTime) => {
  var newTime = new Date(newTime);
  var storedTime = new Date(storedTime);

  return new Promise((resolve, reject) => {
    if (Date.parse(newTime) > Date.parse(storedTime)) {
      resolve({ newUpdate: true });
    } else {
      resolve({ newUpdate: false });
    }
  });
};
