var http = require('http');
var qs = require("querystring");
var fs = require('fs');
var cp = require('child_process');
var url = require('url');

var filepath = '../../submodules/raco/c_test_environment/';
var counter = 0;

http.createServer(function (req, res) {
    parseQuery(req, res);

}).listen(1337, 'localhost');
console.log('Server running at http://localhost:1337/');

function getJSON(req, res, qid, start) {
    var end = new Date().getTime();
    var query_status = {url : 'http://localhost:1337/query?qid='+qid,
		       startTime: start, finishTime: end, status: "SUCCESS",
		       queryId: qid};
    res.writeHead(200, {'Content-Type': 'application/json', 'Location':
			'http://localhost:1337/query?qid='+qid});
    res.write(JSON.stringify(query_status));
    res.end();
}
	

// Parses the query from posted json
function parseQuery(req, res) {
    var start = new Date().getTime();
    var plan;
    var qid = counter;
    console.log("waiting");
    if (req.method == "POST") {
	console.log('post');
	var body = '';
	req.on('data', function(chunk) {
            body += chunk;
	});
	req.on('end', function() {
	    var myriares = JSON.parse(body);
	    
	    plan = myriares['plan'];
	    fs.writeFile(filepath + 'q' + qid + ".cpp", plan, function(err) {
		if (err) {
		    console.log(err);
		}
	    });
	});

	runClang(req, res, qid, start);

	counter++;
   } else {
       res.writeHead(400, {'Content-Type': 'text/html'});
       res.write("nothing?");
       res.end();
   }
}

// runs clang on server
function runClang(req, res, qid, start) {
    var filename = 'q' + qid;
    var options = { encoding: 'utf8', timeout: 0, maxBuffer: 200*1024,
		    killSignal: 'SIGTERM', cwd: filepath, env: null };
    var cmd = 'python runclang.py clang ' + filename;
    cp.exec(cmd, options, function(error, stdout, stderr) {
	console.log('stdout: ' + stdout);
	console.log('stderr: ' + stderr);
	if (error !== null) {
	    console.log('exec error: ' + error);
	}
	console.log(filename + ' done');
	getJSON(req, res, qid, start);
    });
}

