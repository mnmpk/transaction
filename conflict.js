use("test");
//db.trans.drop();
//var session = db.getMongo().startSession();
//session.startTransaction({readConcern: {level: "snapshot"}});
//var transDB = session.getDatabase("test");
//console.log("insert inside transaction");
//console.log(transDB.trans.insertOne({_id: 1}));
//console.log("count document inside transaction", transDB.trans.countDocuments({}));
//console.log("count document outside transaction", db.trans.countDocuments({}));
//console.log("insert outside transaction");
//db.trans.insertOne({_id: 2});
//console.log("count document inside transaction", transDB.trans.countDocuments({}));
//console.log("count document outside transaction", db.trans.countDocuments({}));
//session.commitTransaction();
//console.log("count document after transaction", db.trans.countDocuments({}));

db.trans.drop();
//console.log("initial insert");
//console.log(db.trans.insertOne({_id: 1, v:1}));
var session = db.getMongo().startSession();
session.startTransaction({readConcern: {level: "majority"}});
var transDB = session.getDatabase("test");

console.log("update inside transaction");
console.log(transDB.trans.updateOne({_id:1},{$inc: {v:1}}, {upsert:true})); 
console.log("find inside transaction", transDB.trans.find());
console.log("find outside transaction", db.trans.find());

console.log("update outside transaction");
console.log(db.trans.updateOne({_id:1},{$inc: {v:1}}, {upsert:true})); 
console.log("find inside transaction", transDB.trans.find());
console.log("find outside transaction", db.trans.find());

session.commitTransaction();
console.log("find after transaction committed", db.trans.find());

