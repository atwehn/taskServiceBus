var Promise = require('promise');
var mongodb = require('mongodb');

module.exports = function (ctx, done) {
    var conn = openMongoConnection(ctx.data.MONGO_URL).catch(done.bind(this,'An error occurred.'));
    switch(ctx.data.action) {
        case "raise":
            conn = conn.then(raiseEvent.bind(this,ctx.data.type,ctx.data.eventData));
            break;
        case "get":
            conn = conn.then(getActiveEvents.bind(this,ctx.data.type));
            break;
        case "ack":
            conn = conn.then(acknowledgeEvent.bind(this,ctx.data.id));
            break;
        default:
            conn = Promise.resolve("No action specified");
    }
    conn.then(done.bind(this,null),done.bind(this,'An error occurred.'));
};

function openMongoConnection(connectionString) {
    return new Promise(function (resolve, reject) {
        mongodb.MongoClient.connect(connectionString, function (err, db) {
            if (err) reject(err);
            else resolve(db);
        });
    });
}

function searchMongoCollection(db,collection,query) {
    return new Promise(function(resolve,reject){
        var result = [];
        var cursor = db.collection(collection).find(query);
        cursor.each(function(err,doc){
            if(err) {
                reject(err);
                return;
            }
            if(doc) result.push(doc);
            else resolve(result);
        });
        
    });    
}

function deleteMongoRecords(db,collection,query) {
    return new Promise(function(resolve,reject){
        db.collection(collection).deleteMany(query,function(err,results){
            if(err) {
                reject(err);
                return;
            }
            resolve(results);
        });
    });
}



function raiseEvent(type,eventData,db) {
    var events = db.collection('events');
    events.insert({ type: type, eventData:eventData,created: Date.now(),received:false});
    return true;

}

function getActiveEvents(type,db) {
    var types = type.split(',');
    return searchMongoCollection(db,'events',{type:{$in: types}});
}

function acknowledgeEvent(ids,db) {
    if(!ids) return Promise.reject("Please enter an ID"); 
    var idList = ids.split(',').map(function(id){
        return new mongodb.ObjectID(id);
    }); 
    return deleteMongoRecords(db,'events',{'_id': {$in: idList}});

       
       
       
  

}

