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
var dataArray = [];

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
         //console.log("Sending default data *************************");
         //socket.emit('defaultData', ({ "disease" : "Lyme", "Syphilis" : "0", "Gonorrhea" : "0", "Chlamydia" : "0", "Chancroid" : "0", "Hepatitis B" : "4", "Hepatitis C" : "10", "H1N1 (Swine Flu)" : "50", "Lyme" : "0"}));
      //TODO Move arguments to object, may need to use callback due to async

         
         client.runOp(createDataSet);

         setTimeout(function(){socket.emit('defaultData', dataArray);}, 500);
         
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

//Used to create default data for data array #kennebec stackoverflow
Array.prototype.repeat= function(what, L){
 while(L) this[--L]= what;
 return this;
}

var createDataSet = function (collection, client){
  /* 
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
         collection.drop();
         collection.insert(records, function (err, doc) {
                  //console.log(doc);
              });
         console.log('Number of lines: ' + count);
   });
*/
  
   //Default disease list
   var disAry = ["ADULT LEAD", "AMOEBIASIS", "BOTULISM", "HAEMOPHILUS INFLUENZAE - INVASIVE DISEASE", "HEPATITIS A", "SALMONELLOSIS - NON-TYPHOID (SALMONELLA SPP.)", "SHIGA TOXIN-PRODUCING E.COLI (STEC) - NON O157:H7", "VARICELLA (CHICKENPOX)"];
   var disAvailAry = [];
   //var dataArray = [];
   
   //Perform our initial query of diseases requested. Create array of those available.
   collection.aggregate([
            {$match: {
               DISEASE: {$in :disAry}
               }
            },
           {$group:  {
               _id :{DISEASE : "$DISEASE"}
               }
            }
   ],function setAvailableDisease(err, docs){
      
      if(err)
         console.log("Error: " + err );
      
      docs.forEach(function setAvailDiseaseAry(doc){
         disAvailAry.push(doc._id.DISEASE);
         });
      
      console.log("Diseases found: " + disAvailAry.length);
      disAvailAry.sort();
      console.log(disAvailAry);
      
      
      //Begin structuring our results
      disAvailAry.forEach(function(disName){
         var obj = {"disease" : {"name" : disName, "data" : [].repeat(0, disAvailAry.length)}};
         dataArray.push(obj);
         });
      console.log(dataArray);
      console.log("***************POPULATING DATA*******************");
      
      
      var i = -1;
      //LOOP over each disease in array
      disAvailAry.forEach(function getDiseasePids(disease){
         
         //Select PIDs having this disease name
         collection.aggregate(
               
               {$match: {
                  "DISEASE": disease
                  }
               }
               ,{$group: {_id :{PID : "$PID"}}}
            , function (err, pids){
                  
                  if(err)
                     console.log("Error: " + err );
                  
                  //console.log(pids);
                  
                  i++;//advance to the next data array object (next disease)
                  //LOOP over PIDs array
                  pids.forEach(function getDiseasesFromPid(pidDoc){

                     var pid = pidDoc._id.PID;
                     //console.log(pid);
                     //Select PIDs having this disease name
                     collection.aggregate(
                           
                           {$match: {
                              "PID": pid, DISEASE: {$in :disAry}
                              }
                           },{$group: {_id :{DISEASE : "$DISEASE"}}}
                        , function (err, docs){
                           
                              if(err)
                                 console.log("Error: " + err );
                                
                                 docs.forEach(function setDiseaseData(doc){
                                    
                                    var docDisName = doc._id.DISEASE;
                                    var currentDisObjectName = dataArray[i].disease.name;
                                    /*
                                    console.log("I = " + i);
                                    console.log(docDisName);
                                    console.log(disObjectName);
                                    console.log("************************************************");
                                    */

                                    //TODO investigate bug, results inconsistent. async issue?
                                    if(currentDisObjectName !== docDisName){
                                       dataArray[i].disease.data.forEach(function setThisDiseaseData(element, index, array){
                                          
                                         // console.log(dataArray[index].disease.name);
                                         // console.log(index);
                                          
                                          //Does the current document disease name match this data index's name?
                                          if(dataArray[index].disease.name === docDisName){

                                             array[index]++;
                  
                                          }
                                       
                                       });
                                    }
                                 }); 
                           });    
                  });
            });
         });
      setTimeout(function(){console.log(dataArray);}, 500);
   });
   
   //client.close;

}

var client = new DBClientSingleton.getInstance("mongo", "localhost", 27017);
