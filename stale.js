use("test");
db.trans.drop();
console.log("initial insert");
console.log(db.trans.insertOne({_id: 1, v:1}));
var session = db.getMongo().startSession();
session.startTransaction({readConcern: {level: "majority"}});
var transDB = session.getDatabase("test");
console.log("find inside transaction", transDB.trans.find());
console.log("find outside transaction", db.trans.find());

console.log("update outside transaction");
console.log(db.trans.updateOne({_id:1},{$inc: {v:1}})); 
console.log("find inside transaction", transDB.trans.find(), "<-- data is stale in transaction");
console.log("find outside transaction", db.trans.find());

session.commitTransaction();
console.log("find after transaction committed", db.trans.find());

