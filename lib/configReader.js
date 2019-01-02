/**
 * Free Pascal Pool
 * https://github.com/
 *
 * Configuration Reader
 **/

var fs = require("fs");

/**
 * Read version from package.json
 **/
var packageJson = require('../package.json');
global.version = packageJson.version;

/**
 * Use JSON minify to allow comments in config.json
 **/
JSON.minify = JSON.minify || require("node-json-minify");

/**
 * Get default configuration file or path specified with -config=config-alt.json
 **/
var configFile = (function() {
    for(var i = 0; i < process.argv.length; i++) {
	if(process.argv[i].indexOf("-config=") === 0) {
	    return process.argv[i].split("=")[1];
	}
    }
    return "config.json";
})();

/**
 * Read configuration file data
 **/
try {
    global.config = JSON.parse(
	JSON.minify(
	    fs.readFileSync(configFile).toString()
	)
    );
} catch(e) {
    console.error("Failed to read config file " + configFile + "\nAbort.");
    process.exit();
}
