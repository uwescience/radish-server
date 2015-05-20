// Used to handle http request and parsing fields, actual work done in python
'use strict';

var http = require('http');
var fs = require('fs');
var cp = require('child_process');
var url = require('url');
var express = require('express');
var app = express();
var bodyParser = require('body-parser');


if( !process.env.RACO_HOME ) {
    console.log('Set RACO_HOME to path to raco');
    process.exit(9);
}
var compilepath = process.env.RACO_HOME + '/c_test_environment/';
     
var hostname = 'localhost';
var port = 1337;

var py = './datastore.py';
var counter;
getQid();

// parsing application/json
app.use(bodyParser.json());

app.get('/dataset', function(req, res) {
      processBackend(req, res, selectAll);
});
app.get('/query', function(req, res) {
      processQid(req, res, selectRow);
});
app.get('/status', function(req, res) {
      processQid(req, res, getQueryStatus);
});
app.post('/tuples', function(req, res) {
      processRelKey(req, res, getTuples);
});
app.get('/data', function(req, res) {
    var qid = req.params.query.qid
    getResults(res, qid);
});
app.post('/catalog', function(req, res) {
      processRelKey(req, res, isInCatalog);
});
app.post('/queries', function(req, res) {
      processMinMaxPOST(req, res, selectTable);
});
app.get('/queries', function(req, res) {
      processMinMaxGET(req, res, selectTable);
});
app.post('/new', function(req, res) {
    var json = req.body;
    var uploadinfo = json.uploadinfo;
    console.log("/new got " + json + " AND " + uploadinfo);

    cp.exec(py + ' insert_new_dataset -p ' + uploadinfo, function (err, stdout) {
        if (err) { console.error('processNew ' + err.stack); } 
        else { sendJSONResponse(res, stdout); }
    });
});
app.get('/uploadLocation', function(req, res) {
    var j = {'dir': __dirname};
    sendJSONResponse(res, JSON.stringify(j));
});

// default
app.post('/', function(req, res) {
    // Parses the query from posted json
    var qid = counter++;

    var myriares = req.body;
    var backend = myriares.backend;
    var plan = myriares.plan;
    var relkey = myriares.relkey;
    var query = escape(myriares.rawQuery);
    var url = 'http://' + hostname + ':' + port;
    var filename = relkey.split('_')[2];
    var params = relkey + ' ' + url + ' ' + ' ' + qid + ' ' + backend + ' ' + query;
    
    cp.exec(py + ' process_query -p ' + params, function (err, stdout) {
    if (err) { console.error('process' + err.stack); 
    } else { 
        console.log(stdout); getQueryStatus(res, qid);
    }});
    
    fs.writeFile(compilepath + filename + ".cpp", plan, function (err) {
    if (err) { console.error('writing query source' + err.stack); 
    } else {
        runQueryUpdate(res, filename, qid, backend);
    }});
});
     
var server = app.listen(port, function() {
    var _host = hostname;
    var _port = port;
    console.log('Server running at http://' + hostname + ':' + port + '/');
});

function processRelKey(req, res, callbackfn) {
    var relkey = req.body;
    console.log("relation key: " + JSON.stringify(relkey));
    callbackfn(res, relkey);
}

function processMinMaxPOST(req, res, callbackfn) {
      var json = req.body;
      var min = json.min;
      var max = json.max;
      var backend = json.backend;
      callbackfn(res, backend, min, max);
  }
function processMinMaxGET(req, res, callbackfn) {
    var backend = req.query.backend;
    callbackfn(res, backend, 0, 0);
}


function processQid(req, res, callbackfn) {
    var qid = req.params.query.qid;
    callbackfn(res, qid);
}

function processBackend(req, res, callbackfn) {
    var backend = req.params.query.backend;
    callbackfn(res, backend);
}

function runQueryUpdate(res, filename, qid, backend) {
  var params = qid + ' ' + filename + ' ' + backend;
  datastore_api(res, 'update_query_run', params, function (stdout) {
    console.log(stdout);
  });
}

function isInCatalog(res, rkey) {
  var params = rkey.userName + ' ' + rkey.programName + ' '
        + rkey.relationName;
  datastore_api(res, 'check_catalog', params, function (stdout) {
      sendJSONResponse(res, JSON.stringify(JSON.parse(stdout)));
  });
}

function selectTable(res, backend, min, max) {
  if (min == null) {
      min = '0';
  }
  if (max == null) {
      max = '0';
  }
  var params = min + ' ' + max + ' ' + backend;
  datastore_api(res, 'select_table', params, function (stdout) {
	sendJSONResponse(res, stdout);
  });
}

function selectAll(res, backend) {
   datastore_api(res, 'select_all', backend, function (stdout) {
	sendJSONResponse(res, stdout);
  });
}

function selectRow(res, qid) {
  datastore_api(res, 'select_row', qid, function (stdout) {
      sendJSONResponse(res, JSON.stringify(JSON.parse(stdout)));
  });
}

function getResults(res, qid) {
  datastore_api(res, 'get_filename', qid, function (stdout) {
      sendJSONResponse(res, stdout);
  });
}

function getQueryStatus(res, qid) {
  datastore_api(res, 'get_query_status', qid, function (stdout) {
      sendJSONResponse(res, stdout);
  });
}

function getTuples(res, rkey) {
  var params = rkey.userName + ' ' + rkey.programName + ' '
        + rkey.relationName;
  datastore_api(res, 'get_num_tuples', params, function (stdout) {
      sendJSONResponse(res, stdout);
  });
}

function getQid() {
  cp.exec(py + ' get_latest_qid', function (err, stdout) {
    if (err) {
      console.error( 'getQid ' + err.stack);
      counter = 0;
    } else {
      counter = parseInt(stdout) + 1;
    }
  });
}

function sendJSONResponse(res, json) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.writeHead(200, {'Content-Type': 'application/json'});
  res.write(json);
  res.end();
}

function datastore_api(res, func, params, success) {
    cp.exec(py + ' ' + func + ' -p ' + params, function (err, stdout) {
        if (err) { 
            console.error( func + ' ' + err.stack);
            res.status(500).send( err.stack ); 
        } else {
            success(stdout);
        }
    });
}
