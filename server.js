var express = require('express'),
   fs = require('fs'),
   http = require('http'),
   csv = require('csv'),
   path = require('path');
var records = new Array();
var app = express();
var records = [];



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


http.createServer(app).listen(app.get('port'), function () {
   console.log("Express server listening on port " + app.get('port'));
});