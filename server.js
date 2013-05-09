var express = require('express'),
   fs = require('fs'),
   http = require('http'),
   csv = require('csv'),
   path = require('path'),
   assert = require('assert');
   
var app = express();



var records = new Array();
var records = [];
var dataDir = "/data";


var errors = {
   
   db : {missingparams : "Please specify all parameters to create a connection.",
         typenotsupported : "The DB type is not supported.",
         }
   };

//TODO move to js, use export, require. Check db closure, reopen
// Connect to the db
var DbSingleton = (function(){
   
      var instance = {};
      var dbType = {
         mongo : {name : "mongo", protocol : "mongodb"}
         
         };
         
      function privGetInstance(type, url, port, dbName){
         
         for(k in arguments){
            if(!arguments[k] || arguments[k] === null){
               console.error(errors.db.missingparams);
               break;
            }
         }
 
         if(type === dbType.mongo.name){
            console.info("Trying " + dbType.mongo.name);
            
            var MongoClient = require('mongodb').MongoClient;
            var MongoServer = require('mongodb').Server;
            
            var mclient = new MongoClient(new MongoServer(url, port, {auto_reconnect: true}))
            mclient.open(function(){});
            instance[dbType.mongo.name] = mclient.db(dbName);
            return instance[dbType.mongo.name];
         }
         else{
            console.error(errors.db.typenotsupported);
            return null;
         }
      };
      
      
      return{
         
         getInstance : function(type, url, port, db){
                
                if (!instance[type]){
                  instance[type] = privGetInstance(type, url, port, db);
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



//TODO Move arguments to object
var db = DbSingleton.getInstance("mongo", "localhost", 27017, "final");
var collection = db.collection('disease_by_node');

//TODO work with result set. Manipulate to JSON
collection.find({PID : "39481", DISEASE : {$ne: "PERTUSSIS"}}, {DISEASE:1});
