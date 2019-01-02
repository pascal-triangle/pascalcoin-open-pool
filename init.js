/**
 * Free Pascal Pool
 * https://github.com/
 *
 * Pool initialization script
 **/

var fs = require("fs");
var cluster = require("cluster");
var os = require("os");

// Hmm... figure this out
global.diff1 = 0x00000000ffff0000000000000000000000000000000000000000000000000000;


/**
 * Read config file into global config variable
 **/
require("./lib/configReader.js");


/**
 * Initialize log system
 **/
require("./lib/logger.js");

var logSystem = "master";
require("./lib/exceptionWriter.js")(logSystem);


/**
 * Initialize redis database client
 **/
var redis = require("redis");

var redisDB = (config.redis.database && config.redis.database > 0) ? config.redis.database : 0;
global.redisClient = redis.createClient(config.redis.port, config.redis.host, { db: redisDB, auth_pass: config.redis.auth });
global.redisPrefix = config.redis.prefix;

global.redisClient.on("ready", function() {
    //log("info", logSystem, "Connected to redis on %s:%s db %s", [config.redis.host, config.redis.port, redisDB]);
});
global.redisClient.on("error", function (err) {
    log("error", logSystem, err);
});
global.redisClient.on("end", function() {
    log("error", logSystem, "Connection to redis database has been ended");
});


/**
 * Load pool modules
 **/
if(cluster.isWorker) {
    switch(process.env.workerType) {
    case "daemon":
	new require("./lib/daemon.js").Miner();
	break;
    case "pool":
	require("./lib/pool.js");
	break;
    case "blockUnlocker":
        require("./lib/blockUnlocker.js");
        break;
    case "paymentProcessor":
	require("./lib/paymentProcessor.js");
	break;
    case "api":
	require("./lib/api.js");
	break;
    case "charts":
	require("./lib/chartsDataCollector.js");
	break;
    }
    return;
}


/**
 * Welcome message
 **/
log("info", logSystem, "Starting Free Pascal Pool version %s", [version]);


/**
 * Check if we are running a single component with the -module=X flag
 **/
var singleModule = (function() {
    var validModules = ["pool", "unlocker", "api", "payments", "charts"];

    for(var i = 0; i < process.argv.length; i++) {
	if(process.argv[i].indexOf("-module=") === 0) {
	    var moduleName = process.argv[i].split("=")[1];
	    if(validModules.indexOf(moduleName) > -1)
		return moduleName;

	    log("error", logSystem, "Invalid module '%s', valid modules: %s", [moduleName, validModules.join(", ")]);
	    process.exit();
	}
    }
})();


/**
 * Start modules
 **/
(function init() {
    checkRedisVersion(function() {
	if(singleModule) {
	    log("info", logSystem, "Running in single module mode: %s", [singleModule]);

	    switch(singleModule) {
	    case "pool":
		spawnDaemon();
		spawnPoolWorkers();
		break;
            case "unlocker":
                spawnBlockUnlocker();
                break;
	    case "payments":
		spawnPaymentProcessor();
		break;
	    case "api":
		spawnApi();
		break;
            case "charts":
                spawnChartsDataCollector();
                break;
	    }
	} else {
	    spawnDaemon();
	    spawnPoolWorkers();
            spawnBlockUnlocker();
	    spawnPaymentProcessor();
	    spawnApi();
            spawnChartsDataCollector();
	}
    });
})();


/**
 * Check redis database version
 **/
function checkRedisVersion(callback) {
    redisClient.info(function(error, response) {
	if(error) {
	    log("error", logSystem, "Redis version check failed");
	    return;
	}
	var parts = response.split("\r\n");
	var version;
	var versionString;
	for(var i = 0; i < parts.length; i++) {
	    if(parts[i].indexOf(":") !== -1) {
		var valParts = parts[i].split(":");
		if(valParts[0] === "redis_version") {
		    versionString = valParts[1];
		    version = parseFloat(versionString);
		    break;
		}
	    }
	}
	if(!version) {
	    log("error", logSystem, "Could not detect redis version - must be super old or broken");
	    return;
	} else if(version < 2.6) {
	    log("error", logSystem, "You're using redis version %s the minimum required version is 2.6.", [versionString]);
	    return;
	}
	callback();
    });
}


/**
 * Spawn Daemon interface module
 **/
function spawnDaemon() {
    if(!config.daemon || !config.daemon.host || !config.daemon.miningPort)
	return;

    var worker = cluster.fork({
	workerType: "daemon"
    });
    worker.type = "daemon";

    worker.on("exit", function(code, signal) {
	log("error", logSystem, "Daemon interface died, spawning replacement...");
	setTimeout(function() {
	    spawnDaemon();
	}, 2000);
    }).on("message", function(msg) {
	switch(msg.type) {
	case "minerNotify":
	    Object.keys(cluster.workers).forEach(function(id) {
		if(cluster.workers[id].type === "pool") {
		    cluster.workers[id].send({type: "minerNotify", block: msg.block});
		}
	    });
	    break;
	case "blockAccepted":
	    // probably do something here
	    break;
	}
    });
}


/**
 * Spawn pool workers module
 **/
function spawnPoolWorkers() {
    if(!config.poolServer || !config.poolServer.enabled)
	return;

    if(!config.poolServer.ports || config.poolServer.ports.length === 0) {
	log("error", logSystem, "Pool server enabled but no ports specified");
	return;
    }

    var numForks = (function () {
        if(!config.poolServer.clustering || !config.poolServer.clustering.enabled) {
            return 1;
        }
        if(config.poolServer.clustering.forks === "auto") {
            return os.cpus().length;
        }
        if(!config.poolServer.clustering.forks || isNaN(config.poolServer.clustering.forks)) {
            return 1;
        }
        return config.poolServer.clustering.forks;
    })();

    var poolWorkers = {};

    var createPoolWorker = function(forkId) {
	var worker = cluster.fork({
	    workerType: "pool",
	    forkId: forkId
	});
	worker.forkId = forkId;
	worker.type = "pool";
	poolWorkers[forkId] = worker;

	worker.on("exit", function(code, signal) {
	    log("error", logSystem, "Pool fork %s died, spawning replacement worker...", [forkId]);
	    setTimeout(function() {
		createPoolWorker(forkId);
	    }, 2000);
	});

	worker.on("message", function(msg) {
	    switch(msg.type) {
	    case "submitBlock":
		Object.keys(cluster.workers).forEach(function(id) {
		    if(cluster.workers[id].type === "daemon") {
			cluster.workers[id].send({type: "submitBlock", block: msg.block});
		    }
		});
		break;
	    case "banIP":
		Object.keys(cluster.workers).forEach(function(id) {
		    if(cluster.workers[id].type === "pool") {
			cluster.workers[id].send({type: "banIP", ip: msg.ip});
		    }
		});
		break;
	    }
	});
    };

    var i = 1;
    var spawnInterval = setInterval(function() {
	createPoolWorker(i);
	i++;
	if(i - 1 === numForks) {
	    clearInterval(spawnInterval);
	    log("info", logSystem, "Pool spawned on %d thread(s)", [numForks]);
	}
    }, 10);
}


/**
 * Spawn block unlocker module
 **/
function spawnBlockUnlocker(){
    if (!config.blockUnlocker || !config.blockUnlocker.enabled) return;

    var worker = cluster.fork({
        workerType: "blockUnlocker"
    });
    worker.on("exit", function(code, signal){
        log("error", logSystem, "Block unlocker died, spawning replacement...");
        setTimeout(function(){
            spawnBlockUnlocker();
        }, 2000);
    });
}


/**
 * Spawn payment processor module
 **/
function spawnPaymentProcessor() {
    if(!config.payments || !config.payments.enabled)
	return;

    var worker = cluster.fork({
	workerType: "paymentProcessor"
    });
    worker.on("exit", function(code, signal) {
	log("error", logSystem, "Payment processor died, spawning replacement...");
	setTimeout(function() {
	    spawnPaymentProcessor();
	}, 2000);
    });
}


/**
 * Spawn API module
 **/
function spawnApi(){
    if(!config.api || !config.api.enabled) return;

    var worker = cluster.fork({
        workerType: "api"
    });
    worker.on("exit", function(code, signal){
        log("error", logSystem, "API died, spawning replacement...");
        setTimeout(function(){
            spawnApi();
        }, 2000);
    });
}

/**
 * Spawn charts data collector module
 **/
function spawnChartsDataCollector(){
    if (!config.charts) return;

    var worker = cluster.fork({
        workerType: "charts"
    });
    worker.on("exit", function(code, signal){
        log("error", logSystem, "charts died, spawning replacement...");
        setTimeout(function(){
            spawnChartsDataCollector();
        }, 2000);
    });
}
