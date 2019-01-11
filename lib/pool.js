/**
 * PascalCoin Open Pool
 * https://github.com/PascalCoinPool/pascalcoin-open-pool
 *
 * Pool TCP daemon
 **/

var varDiffManager = require("./varDiffManager.js");
var stratum = require("./stratum.js");
var jobManager = require("./jobManager.js");
var notifications = require("./notifications.js");
var utils = require("./utils.js");

var ShareProcessor = require("./shareProcessor.js");
var shareProcessor = new ShareProcessor();

/**
 * Initialize log system
 **/
var logSystem = "pool";
require("./exceptionWriter.js")(logSystem);

var forkId = process.env.forkId;
var threadId = "(Thread " + forkId + ") ";
var log = function(severity, system, text, data) {
    global.log(severity, system, threadId + text, data);
};


/**
 * Handle multi-thread messages
 **/
process.on("message", function(message) {
    switch (message.type) {
    case "minerNotify":
        jobManager.processTemplate(message.block);
        break;
    case "banIP":
        if(stratumServer) {
            stratumServer.addBannedIP(message.ip);
        }
        break;
    }
});


/**
 * Set-up Ports
 **/
var ports = [];
var varDiff = {};
config.poolServer.ports.forEach(function(portCfg) {
    if(portCfg.port && portCfg.diff) {
	ports[portCfg.port] = portCfg; // adds port information as an array with keys
	if(portCfg.varDiff) {
	    if(typeof(varDiff[portCfg.port]) != "undefined" ) {
		varDiff[portCfg.port].removeAllListeners();
	    }
	    varDiff[portCfg.port] = new varDiffManager(portCfg.port, portCfg.varDiff);
	    varDiff[portCfg.port].on("newDifficulty", function(client, newDiff) {
		/* We request to set the newDiff @ the next difficulty retarget
		   (which should happen when a new job comes in - AKA BLOCK) */
		client.enqueueNextDifficulty(newDiff);
	    });
	}
    } else {
	log("error", logSystem, "Port %s has invalid configuration", [portCfg.port]);
    }
});


/**
 * Set-up Job Manager
 **/
var jobManager = new jobManager();

jobManager.on("newBlock", function(blockTemplate) {
    //Check if stratumServer has been initialized yet
    if(stratumServer) {
	stratumServer.broadcastMiningJobs(blockTemplate.getJobParams());

	var numMiners = Object.keys(stratumServer.getStratumClients()).length
	log("info", logSystem, "Sent new work to %s workers for height %s",
	    [numMiners, blockTemplate.rpcData.block]);
    }
});

jobManager.on("updatedBlock", function(blockTemplate) {
    //Check if stratumServer has been initialized yet
    if(stratumServer) {
	var job = blockTemplate.getJobParams();
	job[8] = false;
	stratumServer.broadcastMiningJobs(job);

	var numMiners = Object.keys(stratumServer.getStratumClients()).length
	log("info", logSystem, "Sent updated work to %s workers for height %s",
	    [numMiners, blockTemplate.rpcData.block]);
    }
});

jobManager.on("share", function(shareData, block) {

    var isValidShare = !shareData.error;
    var isValidBlock = !!block;
    var emitShare = function() {
	emit("share", isValidShare, isValidBlock, shareData);
    };

    if(isValidBlock) {
	process.send({
	    type: "submitBlock",
	    block: block
	});
	log("info", logSystem, "Block found at height %s by %s@%s",
	    [block.height, shareData.worker, shareData.ip]);
    }
    if(isValidShare) {
	if(shareData.shareDiff > 1000000000) {
	    log("warn", logSystem, "Share was found with diff higher than 1,000,000,000!");
	} else if(shareData.shareDiff > 1000000) {
	    log("warn", logSystem, "Share was found with diff higher than 1,000,000!");
	}
	log("info", logSystem, "Share accepted at diff %s/%s from %s@%s",
	    [Number(shareData.difficulty).toFixed(8),
	     Number(shareData.shareDiff).toFixed(8),
	     shareData.worker, shareData.ip]);
    } else {
	log("error", logSystem, "Share rejected from %s@%s", [shareData.worker, shareData.ip]);
	log("error", logSystem, "Data %s", [JSON.stringify(shareData)]);
    }
    shareProcessor.handleShare(isValidShare, isValidBlock, shareData);

});

jobManager.on("log", function(severity, message, data) {
    log(severity, logSystem, message, data);
});


/**
 * Initialize Stratum Server
 * Wait until jobManager has gotten its first job
 * before starting
 **/

var stratumServer = false;

var waitForFirstJob = setInterval(() => {

    if(jobManager.currentJob === false) {
	log("info", logSystem, "Waiting for first work from daemon");
	return;
    }

    clearInterval(waitForFirstJob);

    log("info", logSystem, "Pool thread ready!");

    stratumServer = new stratum.Server(function(ip, port, login, callback) {

        var login_parts = utils.validateLogin(login);

        if(login_parts.valid) {
	    log("info", logSystem, "Miner connected %s@%s on port %s", [login, ip, port]);

	    // Record new miner in Redis and set password in worker_passwords
            var dateNowSeconds = Math.floor(Date.now() / 1000);
            var redisCommands = [
                ["hincrby", redisPrefix + ":ports:" + port, "users", 1],
                ["zadd", redisPrefix + ":worker_passwords:" + login_parts.address_pid, dateNowSeconds, login_parts.password],
                ["hincrby", redisPrefix + ":active_connections", login_parts.address_pid + "~" + login_parts.worker_id, 1]
            ];

            redisClient.multi(redisCommands).exec(function(err, replies) {
		if (replies[2] === 1) {
		    notifications.sendToMiner(login_parts.address_pid, "worker_start", {
			"IP": ip.replace("::ffff:", ""),
			"PORT": port,
                        "ACCOUNT": login_parts.address,
			"WORKER_NAME": login_parts.worker_id
		    });
		}
	    });

	    callback({
	        error: null,
	        authorized: true,
	        disconnect: false
	    });
        } else {
	    log("warn", logSystem, "Invalid login %s@%s on port %s", [login, ip, port]);
	    callback({
	        error: login_parts.message,
	        authorized: false,
	        disconnect: true
	    });
        }

    });

    stratumServer.on("log", function(severity, message, data) {
	log(severity, logSystem, message, data);
    });

    stratumServer.on("started", function() {
	log("info", logSystem, "Stratum server started!");
	stratumServer.broadcastMiningJobs(jobManager.currentJob.getJobParams());
	var numMiners = Object.keys(stratumServer.getStratumClients).length;
	log("info", logSystem, "Sent initial work to %s workers for height %s",
	    [numMiners, jobManager.currentJob.rpcData.block]);
    });

    stratumServer.on("broadcastTimeout", function() {
	jobManager.updateCurrentJob();
	log("warning", logSystem, "No new blocks for %s seconds, rebroadcasting work",
	    [config.poolServer.jobRebroadcastTimeout]);
    });

    stratumServer.on("client.connected", function(client) {

	if(typeof(varDiff[client.socket.localPort]) !== "undefined") {
	    varDiff[client.socket.localPort].manageClient(client);
	}

	client.on("difficultyChanged", function(diff) {
	    log("info", logSystem, "Difficulty update to diff %s workerName = %s",
		[Number(diff).toFixed(8), client.workerName]);
	});

	client.on("subscription", function(resultCallback) {
	    var extraNonce = jobManager.extraNonceCounter.next();
	    var extraNonce2Size = jobManager.extraNonce2Size;
	    resultCallback(null, extraNonce, extraNonce2Size);
	    this.sendDifficulty(ports[client.socket.localPort].diff);
	    this.sendMiningJob(jobManager.currentJob.getJobParams());
	});

	client.on("submit", function(params, resultCallback) {
            var login_parts = utils.validateLogin(params.name);
            if(login_parts.valid) {
	        jobManager.processShare(
		    params.jobId,
		    client.previousDifficulty,
		    client.difficulty,
		    client.extraNonce1,
		    params.extraNonce2,
		    params.nTime,
		    params.nonce,
		    client.remoteAddress,
		    client.socket.localPort,
                    login_parts,
                    function(result) {
	                resultCallback(result.error, result.result ? true : null);
                    }
	        );
            } else {
                var result = {
                    job: params.jobId,
                    ip: client.remoteAddress,
                    worker: params.name,
                    workerName: "",
                    difficulty: client.difficulty,
                    shareDiff: client.difficulty,
                    error: login_parts.message
                }
	        resultCallback(result.error, result.result ? true : null);
            }
	});

	client.on("malformedMessage", function (message) {
            var buffer = Buffer.from(message);
            message = buffer.toString("hex");
	    log("warn", logSystem, "Malformed message from %s: %s", [client.getLabel(), message]);
	});

	client.on("socketError", function(err) {
	    log("warn", logSystem, "Socket error from %s: %s", [client.getLabel(), JSON.stringify(err)]);
	});

	client.on("socketTimeout", function(reason) {
            removeConnectedWorker(client, "timeout");
	    log("warn", logSystem, "Connected timed out for %s: %s", [client.getLabel(), reason]);
	});

	client.on("socketDisconnect", function() {
            removeConnectedWorker(client, "disconnect");
	    log("info", logSystem, "Socket disconnected from %s", [client.getLabel()]);
	});

	client.on("kickedBannedIP", function(remainingBanTime) {
	    log("info", logSystem, "Rejected incoming connection from %s banned for %s more seconds",
		[client.remoteAddress, remainingBanTime]);
	});

	client.on("forgaveBannedIP", function() {
	    log("info", logSystem, "Forgave banned IP %s", [client.remoteAddress]);
	});

	client.on("unknownStratumMethod", function(fullMessage) {
	    log("info", logSystem, "Unknown stratum method from %s: %s", [client.getLabel(), fullMessage.method]);
	});

	client.on("socketFlooded", function() {
	    log("warn", logSystem, "Detected socket flooding from %s", [client.getLabel()]);
	});

	client.on("tcpProxyError", function(data) {
	    log("error", logSystem, "Client IP detection failed, tcpProxyProtocol is enabled yet did not receive proxy protocol message, instead got data: %s", [data]);
	});

	client.on("bootedBannedWorker", function() {
	    log("warn", logSystem, "Booted worker %s who was connected from an IP address that was just banned",
		[client.getLabel()]);
	});

	client.on("triggerBan", function(reason) {
	    process.send({
		type: "banIP",
		ip: client.remoteAddress
	    });
            removeConnectedWorker(client, "banned");
	    log("warn", logSystem, "Banned triggered for %s: %s", [client.getLabel(), reason]);
	});

    });

}, 500);

function removeConnectedWorker(client, reason) {
    redisClient.hincrby(redisPrefix + ":ports:" + client.port, "users", "-1");

    var login_parts = utils.validateLogin(client.workerName);
    redisClient.hincrby(redisPrefix + ":active_connections", login_parts.address_pid + "~" + login_parts.worker_id, -1, function(error, connectedWorkers) {
        if (reason === "banned") {
            notifications.sendToMiner(login_parts.address_pid, "worker_banned", {
		"IP": client.remoteAddress.replace("::ffff:", ""),
		"PORT": client.port,
                "ACCOUNT": login_parts.address,
		"WORKER_NAME": login_parts.worker_id
            });
        } else if (!connectedWorkers || connectedWorkers <= 0) {
            notifications.sendToMiner(login_parts.address_pid, "worker_stop", {
		"IP": client.remoteAddress.replace("::ffff:", ""),
		"PORT": client.port,
                "ACCOUNT": login_parts.address,
		"WORKER_NAME": login_parts.worker_id,
                "LAST_HASH": utils.dateFormat(new Date(client.lastActivity), "yyyy-mm-dd HH:MM:ss Z")
            });
        }
    });
}
