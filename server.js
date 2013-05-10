var express = require('express'),
   fs = require('fs'),
   http = require('http'),
   csv = require('csv'),
   path = require('path'),
   assert = require('assert');
   
var app = express();
var MongoClient = require('mongodb').MongoClient;


var records = new Array();
var records = [];
var dataDir = "/data";


var messages = {
   
      errors: {
         db : {
            missingparams : "Please specify all parameters to create a connection.",
            typenotsupported : "The DB type is not supported.",
            connecterr : "Unable to establish connection to db",
            openerr : "The following error occurred on open: ",
            colerr : "The following error occurred on collection retrieval: ",
         }
      },  
      info : {
         db: {
            connected : "Connection successful.",
            opened : "Open successful.",
            
         }
         
      }  
};

var mdbParams = {
      dbName : "final",
      dbColName : "disease_by_node"
   };


//TODO move to js, use export, require.
// Connect to the db
var DBClientSingleton = (function(){
   
      var instance = {};
      var dbType = {
         mongo : {name : "mongo", protocol : "mongodb"}
         
         };
         
      function privGetInstance(type, url, port){
         
         for(k in arguments){
            if(!arguments[k]){
               console.error(messages.errors.db.missingparams);
               return null;
            }
         }
 
         if(type === dbType.mongo.name){
            console.info("Trying " + dbType.mongo.name);
            var MongoServer = require('mongodb').Server;
            var mclient = new MongoClient(new MongoServer(url, port, {auto_reconnect: true}))

            if(mclient)
               console.info(messages.info.db.connected);
            else
               console.info(messages.errors.db.connecterr);
               
            instance[dbType.mongo.name] = mclient;
            return instance[dbType.mongo.name];
         }
         else{
            console.error(messages.errors.db.typenotsupported);
            return null;
         }
      };
      return{
         
         getInstance : function(type, url, port){
                
                if (!instance[type]){
                  instance[type] = privGetInstance(type, url, port);
                }

                return instance[type];
            }
         };
   
   })();



csv(records)
   //.from.stream(fs.createReadStream(__dirname + dataDir +'/contract.txt'), {
   //.from.stream(fs.createReadStream(__dirname + dataDir +'/diseaselinks.csv'), {
   //.from.stream(fs.createReadStream(__dirname + dataDir + '/diseaselinks_onemode_valued.csv'), {
   .from.stream(fs.createReadStream(__dirname + dataDir + '/disease_association_by_node.csv'), {
   columns: true
})
   .on('record', function (row, index) {
   records.push(row);

   //console.log(row);
})
   .on('end', function (count) {
      //collection.insert(records, function (err, doc) {
               //console.log(doc);
       //     });
      console.log('Number of lines: ' + count);
});


app.configure(function () {
   app.set('port', process.env.PORT || 2600);
   app.use(express.favicon());
   app.use(express.logger('dev'));
   app.use(express.bodyParser());
   app.use(express.methodOverride());
   app.use(app.router);
   app.use(express.static(path.join(__dirname, 'public')));
});

app.configure('development', function () {
   app.use(express.errorHandler());
});


var server = http.createServer(app).listen(app.get('port'), function () {
   console.log("Express server listening on port " + app.get('port'));
});

var io = require('socket.io').listen(server);
   
   
io.sockets.on('connection', function(socket){
   socket.on('getData', function(){
         console.log("Sending default data *************************");
         //socket.emit('defaultData', ({ "disease" : "Lyme", "Syphilis" : "0", "Gonorrhea" : "0", "Chlamydia" : "0", "Chancroid" : "0", "Hepatitis B" : "4", "Hepatitis C" : "10", "H1N1 (Swine Flu)" : "50", "Lyme" : "0"}));
   
   });
});


MongoClient.prototype.runOp = function(func){
   
   var client = this;
   
   //Open a connection to the db
   client.open(function(err, client){
      
         if(err)
            console.error(messages.errors.db.openerr + err);
         
         //Choose the DB to use
         var db = client.db(mdbParams.dbName);
         
         //Open the collection and perform the passed operation on it
         db.collection(mdbParams.dbColName, function(err, col){
            
            if(err)
               console.error(messages.errors.db.colerr);
            
            //Pass the collection to the query function, along with the client to close when done.
            //Need to create extendable base class which handles callback, close implicitly
            func(col, client);

         });

   });
   
   
};


var createDataSet = function (collection, client){
   

   //Default disease list
   var disAry = ["ADULT LEAD", "AMOEBIASIS", "BOTULISM", "HAEMOPHILUS INFLUENZAE - INVASIVE DISEASE", "HEPATITIS A", "SALMONELLOSIS - NON-TYPHOID (SALMONELLA SPP.)", "SHIGA TOXIN-PRODUCING E.COLI (STEC) - NON O157:H7", "VARICELLA (CHICKENPOX)"];
   //var disAvailSet = collection.distinct({DISEASE : {$in : disAry}});
   
   var result = collection.find({DISEASE :  "AMOEBIASIS"});
   
   //console.log(result);
   var items;
   result.toArray(function(err, bson){

       items = bson;
       console.log(items);
    });
      
   
   console.log("************************DONE ***************************");
   
   client.close;

}

//TODO Move arguments to object, may need to use callback due to async

var client = DBClientSingleton.getInstance("mongo", "localhost", 27017);
client.runOp(createDataSet);