use("test");
db.trans.drop();
var session = db.getMongo().startSession();
session.startTransaction({readConcern: {level: "majority"}, writeConcern: { w: "majority" }});
var transDB = session.getDatabase("test");

console.log("insert outside transaction");
console.log(db.trans.insertOne({_id: 1}));
console.log("find inside transaction", transDB.trans.find());
console.log("find outside transaction", db.trans.find());

console.log("insert inside the transaction");
console.log(transDB.trans.insertOne({_id: 2}), "<-- Here!!!");
console.log("find inside transaction", transDB.trans.find());
console.log("find outside transaction", db.trans.find());

console.log("insert outside the transaction");
console.log(db.trans.insertOne({_id: 3}), "<-- Document level concurrency therefore no conflict with the transaction");
console.log("find inside transaction", transDB.trans.find());
console.log("find outside transaction", db.trans.find());

session.commitTransaction();
console.log("find after transaction committed", db.trans.find());