use("test");
console.log("update outside transaction");
console.log(db.trans.updateOne({_id:1},{$inc: {v:1}}, {upsert:true})); 
console.log("find outside transaction", db.trans.find());