/**
 * Free Pascal Pool
 * https://github.com/
 *
 * Charts data collector
 **/

// Load required modules
var fs = require("fs");
var async = require("async");
var http = require("http");

var charts = require("./charts.js");

// Initialize log system
var logSystem = "charts";
require("./exceptionWriter.js")(logSystem);

/**
 * Run charts data collector
 **/
log("info", logSystem, "Started");
charts.startDataCollectors();
