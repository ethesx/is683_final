var express = require('express'),
   fs = require('fs'),
   http = require('http'),
   csv = require('csv'),
   path = require('path');
   
var app = express();

var records = new Array();
var records = [];
var dataDir = "/data";


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
   var MongoClient = require('mongodb').MongoClient;
   // Connect to the db
   MongoClient.connect("mongodb://localhost:27017/final", function (err, db) {
      var collection = db.collection('disease_by_node')
      collection.insert(records, function (err, doc) {
         console.log(doc);
      });
   });
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
         socket.emit('defaultData', ({ "disease" : "Lyme", "Syphilis" : "0", "Gonorrhea" : "0", "Chlamydia" : "0", "Chancroid" : "0", "Hepatitis B" : "4", "Hepatitis C" : "10", "H1N1 (Swine Flu)" : "50", "Lyme" : "0"}));
   
   });
});

/*Migrate this sql to mongo
 *
 *SELECT disease, count(disease) cnt
FROM disease_by_node
WHERE PID IN
  (
    SELECT PID
    FROM disease_by_node
    WHERE disease = 'PERTUSSIS'
  )
AND disease <> 'PERTUSSIS'
GROUP BY disease
order by cnt desc;

*/


//Move to factory
var mdb = require('mongodb').MongoClient;
mdb.connect("mongodb://localhost:27017/final", function (err, db) {
   var disease_by_node = db.collection('disease_by_node');
   
   var recs = disease_by_node.find({DISEASE : "PERTUSSIS"});
   console.log("RETURNED RECORDS: **************************************" + recs);
//iterate over rec results
//db.disease_by_node.find({PID : "39481", DISEASE : {$ne: "PERTUSSIS"}}, {DISEASE:1});
   });
      
