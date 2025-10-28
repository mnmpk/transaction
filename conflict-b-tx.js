use("test");
var session = db.getMongo().startSession();
session.startTransaction({readConcern: {level: "majority"}, writeConcern: { w: "majority" }});
var transDB = session.getDatabase("test");

console.log("update inside transaction");
console.log(transDB.trans.updateOne({_id:1},{$inc: {v:1}}, {upsert:true})); 
console.log("find inside transaction", transDB.trans.find());
console.log("find outside transaction", db.trans.find());

session.commitTransaction();
console.log("find after transaction committed", db.trans.find());