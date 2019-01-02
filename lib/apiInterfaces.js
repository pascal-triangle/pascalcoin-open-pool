/**
 * Free Pascal Pool
 * https://github.com/
 *
 * Handle communications to APIs
 **/

var http = require("http");
var https = require("https");

/**
 * Send API request using JSON HTTP
 **/
function jsonHttpRequest(host, port, data, callback, path) {
    path = path || "/json_rpc";
    callback = callback || function() {};

    var options = {
        hostname: host,
        port: port,
        path: path,
        method: data ? "POST" : "GET",
        headers: {
            "Content-Length": data.length,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    };

    var req = (port === 443 ? https : http).request(options, function(res) {
        var replyData = "";
        res.setEncoding("utf8");
        res.on("data", function(chunk) {
            replyData += chunk;
        });
        res.on("end", function() {
            var replyJson;
            try{
                replyJson = JSON.parse(replyData);
            }
            catch(e) {
                callback(e, {});
                return;
            }
            callback(null, replyJson);
        });
    });

    req.on("error", function(e) {
        callback(e, {});
    });

    req.end(data);
}

/**
 * Exports API interfaces functions
 **/
module.exports = {
    pool: function(path, callback) {
        var bindIp = config.api.bindIp ? config.api.bindIp : "0.0.0.0";
        var host = bindIp !== "0.0.0.0" ? config.api.bindIp : "127.0.0.1";
        var port = config.api.port;
        jsonHttpRequest(host, port, '', callback, path);
    },
    jsonHttpRequest: jsonHttpRequest
};
