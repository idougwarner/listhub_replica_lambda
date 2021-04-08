/*

var dateStart = new Date("2019-12-18T13:55:20.000Z");
var dateEnd = new Date("2019-12-18T13:55:21.000Z");

*/

// Newer time A should be more than older time B
module.exports.istimeANewerthantimeB = (timeA, timeB) => {

    var timeA = new Date(timeA);
    var timeB = new Date(timeB);

    return new Promise(function (resolve, reject) {
        if (Date.parse(timeA) > Date.parse(timeB)) {
            resolve({ "newUpdate": true });
        }
        else {
            resolve(new Error("No new update"));
        }
    });


}