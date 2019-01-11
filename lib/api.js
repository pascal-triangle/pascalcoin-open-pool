/**
 * Free Pascal Pool
 * https://github.com/
 *
 * Pool API
 **/

var fs = require("fs");
var http = require("http");
var https = require("https");
var url = require("url");
var async = require("async");
var dateFormat = require("dateformat");
var bignum = require("bignum");
var base58 = require("base58-native");

// get daemon RPC instance
var daemon = require("./daemon.js");
var daemonRpc = new daemon.Rpc();

var apiInterfaces = require("./apiInterfaces.js");
var authSid = Math.round(Math.random() * 10000000000) + "" + Math.round(Math.random() * 10000000000);

var charts = require("./charts.js");
var notifications = require("./notifications.js");
var market = require("./market.js");
var utils = require("./utils.js");

// Initialize log system
var logSystem = "api";
require("./exceptionWriter.js")(logSystem);

// Data storage variables used for live statistics
var currentStats = {};
var minerStats = {};
var minersHashrate = {};

var liveConnections = {};
var addressConnections = {};

/**
 * Handle server requests
 **/
function handleServerRequest(request, response) {
    var urlParts = url.parse(request.url, true);

    switch(urlParts.pathname) {
        // Pool statistics
    case "/stats":
        handleStats(urlParts, request, response);
        break;
    case "/live_stats":
        response.writeHead(200, {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "Connection": "keep-alive"
        });

        var address = "undefined";
        if(urlParts.query.address) {
            var login_parts = utils.validateLogin(urlParts.query.address);
            if(login_parts.valid) {
                address = login_parts.address_pid;
            }
        }

        var uid = Math.random().toString();
        var key = address + ":" + uid;

        response.on("finish", function() {
            delete liveConnections[key];
        });
        response.on("close", function() {
            delete liveConnections[key];
        });

        liveConnections[key] = response;
        break;

        // Worker statistics
    case "/stats_address":
        handleMinerStats(urlParts, response);
        break;

        // Payments
    case "/get_payments":
        handleGetPayments(urlParts, response);
        break;

        // Pasa Payments
    case "/get_pasa_payments":
        handleGetPasaPayments(urlParts, response);
        break;

        // Blocks
    case "/get_blocks":
        handleGetBlocks(urlParts, response);
        break;

        // Get market prices
    case "/get_market":
	handleGetMarket(urlParts, response);
	break;

        // Top miners
    case "/get_top_miners":
        handleTopMiners(urlParts, response);
        break;

        // Miner settings
    case "/get_miner_settings":
        handleGetMinerSettings(urlParts, response);
        break;
    case "/set_miner_settings":
        handleSetMinerSettings(urlParts, response);
        break;

        // Miners/workers hashrate (used for charts)
    case "/miners_hashrate":
        if(!authorize(request, response)) {
            return;
        }
        handleGetMinersHashrate(response);
        break;
    case "/workers_hashrate":
        if(!authorize(request, response)) {
            return;
        }
        handleGetWorkersHashrate(response);
        break;

        // Pool Administration
    case "/admin_stats":
        if(!authorize(request, response))
            return;
        handleAdminStats(response);
        break;
    case "/admin_monitoring":
        if(!authorize(request, response)) {
            return;
        }
        handleAdminMonitoring(response);
        break;
    case "/admin_log":
        if(!authorize(request, response)) {
            return;
        }
        handleAdminLog(urlParts, response);
        break;
    case "/admin_users":
        if(!authorize(request, response)) {
            return;
        }
        handleAdminUsers(response);
        break;
    case "/admin_ports":
        if(!authorize(request, response)) {
            return;
        }
        handleAdminPorts(response);
        break;

        // Test notifications
    case "/test_email_notification":
        if(!authorize(request, response)) {
            return;
        }
	handleTestEmailNotification(urlParts, response);
        break;

        // Default response
    default:
        response.writeHead(404, {
            "Access-Control-Allow-Origin": "*"
        });
        response.end("Invalid API call");
        break;
    }
}

/**
 * Collect statistics data
 **/
function collectStats() {
    var startTime = Date.now();
    var redisFinished;
    var daemonFinished;

    var redisCommands = [
        ["zremrangebyscore", redisPrefix + ":hashrate", "-inf", ""],
        ["zrange", redisPrefix + ":hashrate", 0, -1],
        ["hgetall", redisPrefix + ":stats"],
        ["zrange", redisPrefix + ":blocks:candidates", 0, -1, "WITHSCORES"],
        ["zrevrange", redisPrefix + ":blocks:matured", 0, config.api.blocks - 1, "WITHSCORES"],
        ["hgetall", redisPrefix + ":scores:roundCurrent"],
        ["hgetall", redisPrefix + ":stats"],
        ["zcard", redisPrefix + ":blocks:matured"],
        ["zrevrange", redisPrefix + ":payments:all", 0, config.api.payments - 1, "WITHSCORES"],
        ["zcard", redisPrefix + ":payments:all"],
        ["keys", redisPrefix + ":payments:*"],
        ["zrevrange", redisPrefix + ":pasaPayments:all", 0, config.api.payments - 1, "WITHSCORES"],
        ["zcard", redisPrefix + ":pasaPayments:all"],
        ["keys", redisPrefix + ":pasaPayments:*"],
        ["hgetall", redisPrefix + ":shares:roundCurrent"],
    ];

    var windowTime = (((Date.now() / 1000) - config.api.hashrateWindow) | 0).toString();
    redisCommands[0][3] = "(" + windowTime;

    async.parallel({
        pool: function(callback) {
            redisClient.multi(redisCommands).exec(function(error, replies) {
                redisFinished = Date.now();
                var dateNowSeconds = Date.now() / 1000 | 0;

                if(error) {
                    log("error", logSystem, "Error getting redis data %j", [error]);
                    callback(true);
                    return;
                }

                var data = {
                    stats: replies[2],
                    blocks: replies[3].concat(replies[4]),
                    totalBlocks: parseInt(replies[7]) + (replies[3].length / 2),
                    totalDiff: 0,
                    totalShares: 0,
                    payments: replies[8],
                    totalPayments: parseInt(replies[9]),
                    totalMinersPaid: replies[10] && replies[10].length > 0 ? replies[10].length - 1 : 0,
                    pasaPayments: replies[11],
                    totalPasaPayments: parseInt(replies[12]),
                    totalPasaMinersPaid: replies[13] && replies[13].length > 0 ? replies[13].length - 1 : 0,
                    miners: 0,
                    workers: 0,
                    hashrate: 0,
                    roundScore: 0,
                    roundHashes: 0
                };

                for(var i = 0; i < data.blocks.length; i++) {
                    var block = data.blocks[i].split(":");
                    if(block[5]) {
                        var blockShares = parseFloat(block[4]);
                        var blockDiff = parseFloat(block[3]);
                        data.totalDiff += blockDiff;
                        data.totalShares += blockShares;
                    }
                }

                minerStats = {};
                minersHashrate = {};

                var hashrates = replies[1];
                for(var i = 0; i < hashrates.length; i++) {
                    var hashParts = hashrates[i].split(":");
                    minersHashrate[hashParts[1]] = (minersHashrate[hashParts[1]] || 0) + parseFloat(hashParts[0]);
                }

                var totalShares = 0;

                for(var miner in minersHashrate) {
                    if(miner.indexOf("~") !== -1) {
                        data.workers++;
                    } else {
                        totalShares += minersHashrate[miner];
                        data.miners++;
                    }

                    minersHashrate[miner] = Math.round(minersHashrate[miner] / config.api.hashrateWindow);

                    if(!minerStats[miner]) { minerStats[miner] = {}; }
                    minerStats[miner]["hashrate"] = minersHashrate[miner];
                }

                data.hashrate = Math.round(totalShares / config.api.hashrateWindow);

                data.roundScore = 0;

                if(replies[5]) {
                    for(var miner in replies[5]) {
                        var roundScore = parseFloat(replies[5][miner]);

                        data.roundScore += roundScore;

                        if(!minerStats[miner]) { minerStats[miner] = {}; }
                        minerStats[miner]["roundScore"] = roundScore;
                    }
                }

                data.roundHashes = 0;

                if(replies[14]) {
                    for(var miner in replies[14]) {
                        var roundHashes = parseFloat(replies[14][miner])
                        data.roundHashes += roundHashes;

                        if(!minerStats[miner]) { minerStats[miner] = {}; }
                        minerStats[miner]["roundHashes"] = roundHashes;
                    }
                }

                if(replies[6]) {
                    data.lastBlockFound = replies[6].lastBlockFound;
                }

                callback(null, data);
            });
        },
        lastblock: function(callback) {
            getLastBlockData(function(error, data) {
                daemonFinished = Date.now();
                callback(error, data);
            });
        },
        network: function(callback) {
            getNetworkData(function(error, data) {
                daemonFinished = Date.now();
                callback(error, data);
            });
        },
        config: function(callback) {
            var configData =  {
                poolHost: config.poolHost || "",
                //ports: getPublicPorts(config.poolServer.ports),
                algorithm: config.algorithm,
                hashrateWindow: config.api.hashrateWindow,
                fee: config.fees.poolFee,
                devFee: config.fees.devFee,
                coin: config.coin,
                symbol: config.symbol,
                depth: config.blockUnlocker.depth,
                version: version,
                pasaThreshold: config.payments.pasaThreshold,
                paymentsInterval: config.payments.interval,
                defaultPaymentThreshold: config.payments.defaultPayment,
                minPaymentThreshold: config.payments.minPayment,
                maxPaymentThreshold: config.payments.maxPayment || null,
                transferFee: config.fees.transferFee,
                sendEmails: config.email ? config.email.enabled : false,
                blocksChartEnabled: (config.charts.pool.blocks && config.charts.pool.blocks.enabled),
                blocksChartDays: config.charts.pool.blocks && config.charts.pool.blocks.days ? config.charts.pool.blocks.days : null,
                knownAccounts: config.knownAccounts,
                passwordExpireTime: config.poolServer.passwordExpireTime
            }
            getPublicPorts(config.poolServer.ports, function(ports) {
                configData.ports = ports;
                callback(null, configData);
            });
        },
        charts: function(callback) {
            // Get enabled charts data
            charts.getPoolChartsData(function(error, data) {
                if(error) {
                    callback(error, data);
                    return;
                }

                // Blocks chart
                if(!config.charts.pool.blocks || !config.charts.pool.blocks.enabled || !config.charts.pool.blocks.days) {
                    callback(error, data);
                    return;
                }

                var chartDays = config.charts.pool.blocks.days;

                var beginAtTimestamp = (Date.now() / 1000) - (chartDays * 86400);
                var beginAtDate = new Date(beginAtTimestamp * 1000);
                if(chartDays > 1) {
                    beginAtDate = new Date(beginAtDate.getFullYear(), beginAtDate.getMonth(), beginAtDate.getDate(), 0, 0, 0, 0);
                    beginAtTimestamp = beginAtDate / 1000 | 0;
                }

                var blocksCount = {};
                if(chartDays === 1) {
                    for(var h = 0; h <= 24; h++) {
                        var date = dateFormat(new Date((beginAtTimestamp + (h * 60 * 60)) * 1000), "yyyy-mm-dd HH:00");
                        blocksCount[date] = 0;
                    }
                } else {
                    for(var d = 0; d <= chartDays; d++) {
                        var date = dateFormat(new Date((beginAtTimestamp + (d * 86400)) * 1000), "yyyy-mm-dd");
                        blocksCount[date] = 0;
                    }
                }

                redisClient.zrevrange(redisPrefix + ":blocks:matured", 0, -1, "WITHSCORES", function(err, result) {
                    for(var i = 0; i < result.length; i++) {
                        var block = result[i].split(":");
                        if(block[6] == 0 || block[6] == 2) {
                            var blockTimestamp = block[2];
                            if(blockTimestamp < beginAtTimestamp) {
                                continue;
                            }
                            var date = dateFormat(new Date(blockTimestamp * 1000), "yyyy-mm-dd");
                            if(chartDays === 1) dateFormat(new Date(blockTimestamp * 1000), "yyyy-mm-dd HH:00");
                            if(!blocksCount[date]) blocksCount[date] = 0;
                            blocksCount[date] ++;
                        }
                    }
                    data.blocks = blocksCount;
                    callback(error, data);
                });
            });
        }
    }, function(error, results) {
        log("info", logSystem, "Stat collection finished: %d ms redis, %d ms daemon", [redisFinished - startTime, daemonFinished - startTime]);

        if(error) {
            log("error", logSystem, "Error collecting all stats");
        }
        else{
            currentStats = results;
            broadcastLiveStats();
        }

        setTimeout(collectStats, config.api.updateInterval * 1000);
    });

}

/**
 * Get Network data
 **/
function getNetworkData(callback) {
    /*
    daemonRpc.async("nodestatus")
        .then((data) => {
            if(data.hasOwnProperty("error")) {
                log("error", logSystem, "Error getting nodestatus data %j", [JSON.stringify(data.error)]);
                callback(true);
                return;
            }
            callback(null, {
                height: data.result.blocks
            });
        });
    */
    daemonRpc.async("getblocks", {"last": 1})
        .then((data) => {
            if(data.hasOwnProperty("error")) {
                log("error", logSystem, "Error getting last block data %j", [JSON.stringify(data.error)]);
                callback(true);
                return;
            }
            var blockHeader = data.result[0];

            var target = utils.targetFromCompact(blockHeader.target);
            var diff = diff1 / target;

            // Block object provides hashratekhs but on testnet we calculate it manually
            if(blockHeader.hashratekhs == 0) {
                var hashrate = Math.pow(2, 32) * diff / 30;
            } else {
                var hashrate = blockHeader.hashratekhs * 1000;
            }

            callback(null, {
                difficulty: diff.toFixed(9),
                hashrate: hashrate,
                height: blockHeader.block + 1,
            });
        });

}


/**
 * Get Last Block data
 **/
function getLastBlockData(callback) {
    daemonRpc.async("getblocks", {"last": 1})
        .then((data) => {
            if(data.hasOwnProperty("error")) {
                log("error", logSystem, "Error getting last block data %j", [JSON.stringify(data.error)]);
                callback(true);
                return;
            }
            var blockHeader = data.result[0];
            callback(null, {
                difficulty: blockHeader.target,
                height: blockHeader.block,
                timestamp: blockHeader.timestamp,
                reward: blockHeader.reward + blockHeader.fee,
                hash:  blockHeader.pow
            });
        });
}

/**
 * Broadcast live statistics
 **/
function broadcastLiveStats() {
    log("info", logSystem, "Broadcasting to %d visitors and %d address lookups", [Object.keys(liveConnections).length, Object.keys(addressConnections).length]);

    // Live statistics
    var processAddresses = {};
    for(var key in liveConnections) {
        var addrOffset = key.indexOf(":");
        var address = key.substr(0, addrOffset);
        if(!processAddresses[address]) processAddresses[address] = [];
        processAddresses[address].push(liveConnections[key]);
    }

    for(var address in processAddresses) {
        var data = currentStats;

        data.miner = {};
        if(address && minerStats[address]) {
            data.miner = minerStats[address];
        }

        var destinations = processAddresses[address];
        sendLiveStats(data, destinations);
    }

    // Workers Statistics
    var processAddresses = {};
    for(var key in addressConnections) {
        var addrOffset = key.indexOf(":");
        var address = key.substr(0, addrOffset);
        if(!processAddresses[address]) processAddresses[address] = [];
        processAddresses[address].push(addressConnections[key]);
    }

    for(var address in processAddresses) {
        broadcastWorkerStats(address, processAddresses[address]);
    }
}

/**
 * Takes a chart data JSON string and uses it to compute the average over the past hour, 6 hours,
 * and 24 hours.  Returns [AVG1, AVG6, AVG24].
 **/
function extractAverageHashrates(chartdata) {
    var now = new Date() / 1000 | 0;

    var sums = [0, 0, 0]; // 1h, 6h, 24h
    var counts = [0, 0, 0];

    var sets = JSON.parse(chartdata); // [time, avgValue, updateCount]
    for(var j in sets) {
        var hr = sets[j][1];
        if(now - sets[j][0] <=  1*60*60) { sums[0] += hr; counts[0]++; }
        if(now - sets[j][0] <=  6*60*60) { sums[1] += hr; counts[1]++; }
        if(now - sets[j][0] <= 24*60*60) { sums[2] += hr; counts[2]++; }
    }

    return [sums[0] * 1.0 / (counts[0] || 1), sums[1] * 1.0 / (counts[1] || 1), sums[2] * 1.0 / (counts[2] || 1)];
}

/**
 * Broadcast worker statistics
 **/
function broadcastWorkerStats(address, destinations) {
    var login_parts = utils.validateLogin(address);
    if(!login_parts.valid) {
        sendLiveStats({error: "Not found"}, destinations);
        return;
    }
    var address = login_parts.address_pid;

    var redisCommands = [
        ["hgetall", redisPrefix + ":workers:" + address],
        ["zrevrange", redisPrefix + ":payments:" + address, 0, config.api.payments - 1, "WITHSCORES"],
        ["zrevrange", redisPrefix + ":pasaPayments:" + address, 0, config.api.payments - 1, "WITHSCORES"],
        ["keys", redisPrefix + ":unique_workers:" + address + "~*"],
        ["get", redisPrefix + ":charts:hashrate:" + address]
    ];
    redisClient.multi(redisCommands).exec(function(error, replies) {
        if(error || !replies || !replies[0]) {
            sendLiveStats({error: "Not found"}, destinations);
            return;
        }

        var stats = replies[0];
        stats.hashrate = minerStats[address] && minerStats[address]["hashrate"] ? minerStats[address]["hashrate"] : 0;
        stats.roundScore = minerStats[address] && minerStats[address]["roundScore"] ? minerStats[address]["roundScore"] : 0;
        stats.roundHashes = minerStats[address] && minerStats[address]["roundHashes"] ? minerStats[address]["roundHashes"] : 0;
        if(replies[4]) {
            var hr_avg = extractAverageHashrates(replies[4]);
            stats.hashrate_1h  = hr_avg[0];
            stats.hashrate_6h  = hr_avg[1];
            stats.hashrate_24h = hr_avg[2];
        }

        var paymentsData = replies[1];
        var pasaPaymentsData = replies[2];

        var workersData = [];
        for(var j=0; j<replies[3].length; j++) {
            var key = replies[3][j];
            var keyParts = key.split(":");
            var miner = keyParts[2];
            if(miner.indexOf("~") !== -1) {
                var workerName = miner.substr(miner.indexOf("~")+1, miner.length);
                var workerData = {
                    name: workerName,
                    hashrate: minerStats[miner] && minerStats[miner]["hashrate"] ? minerStats[miner]["hashrate"] : 0
                };
                workersData.push(workerData);
            }
        }

        charts.getUserChartsData(address, paymentsData, function(error, chartsData) {
            var redisCommands = [];
            for(var i in workersData) {
                redisCommands.push(["hgetall", redisPrefix + ":unique_workers:" + address + "~" + workersData[i].name]);
                redisCommands.push(["get", redisPrefix + ":charts:worker_hashrate:" + address + "~" + workersData[i].name]);
            }
            redisClient.multi(redisCommands).exec(function(error, replies) {
                for(var i in workersData) {
                    var wi = 2*i;
                    var hi = wi + 1
                    if(replies[wi]) {
                        workersData[i].lastShare = replies[wi]["lastShare"] ? parseInt(replies[wi]["lastShare"]) : 0;
                        workersData[i].hashes = replies[wi]["hashes"] ? parseInt(replies[wi]["hashes"]) : 0;
                    }
                    if(replies[hi]) {
                        var avgs = extractAverageHashrates(replies[hi]);
                        workersData[i]["hashrate_1h"]  = avgs[0];
                        workersData[i]["hashrate_6h"]  = avgs[1];
                        workersData[i]["hashrate_24h"]  = avgs[2];
                    }
                }

                var data = {
                    account: login_parts,
                    stats: stats,
                    payments: paymentsData,
                    pasaPayments: pasaPaymentsData,
                    charts: chartsData,
                    workers: workersData
                };

                sendLiveStats(data, destinations);
            });
        });
    });
}

/**
 * Send live statistics to specified destinations
 **/
function sendLiveStats(data, destinations) {
    if(!destinations) return ;

    var dataJSON = JSON.stringify(data);
    for(var i in destinations) {
        destinations[i].end(dataJSON);
    }
}

/**
 * Return pool statistics
 **/
function handleStats(urlParts, request, response) {
    var data = currentStats;

    data.miner = {};
    var address = urlParts.query.address;
    if(address && minerStats[address]) {
        data.miner = minerStats[address];
    }

    var dataJSON = JSON.stringify(data);

    response.writeHead("200", {
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(dataJSON, "utf8")
    });
    response.end(dataJSON);
}

/**
 * Return miner (worker) statistics
 **/
function handleMinerStats(urlParts, response) {
    var login_parts = utils.validateLogin(urlParts.query.address);
    if(!login_parts.valid) {
        var dataJSON = JSON.stringify({error: "Not found"});
        response.writeHead("200", {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "Content-Length": Buffer.byteLength(dataJSON, "utf8")
        });
        response.end(dataJSON);
        return;
    }
    var address = login_parts.address_pid;
    var longpoll = (urlParts.query.longpoll === "true");

    if(longpoll) {
        response.writeHead(200, {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "Connection": "keep-alive"
        });

        redisClient.exists(redisPrefix + ":workers:" + address, function(error, result) {
            if(!result) {
                response.end(JSON.stringify({error: "Not found"}));
                return;
            }

            var uid = Math.random().toString();
            var key = address + ":" + uid;

            response.on("finish", function() {
                delete addressConnections[key];
            });
            response.on("close", function() {
                delete addressConnections[key];
            });

            addressConnections[key] = response;
        });
    } else {
        redisClient.multi([
            ["hgetall", redisPrefix + ":workers:" + address],
            ["zrevrange", redisPrefix + ":payments:" + address, 0, config.api.payments - 1, "WITHSCORES"],
            ["zrevrange", redisPrefix + ":pasaPayments:" + address, 0, config.api.payments - 1, "WITHSCORES"],
            ["keys", redisPrefix + ":unique_workers:" + address + "~*"],
            ["get", redisPrefix + ":charts:hashrate:" + address],
            ["get", redisPrefix + ":charts:shares:" + address]
        ]).exec(function(error, replies) {
            if(error || !replies[0]) {
                var dataJSON = JSON.stringify({error: "Not found"});
                response.writeHead("200", {
                    "Access-Control-Allow-Origin": "*",
                    "Cache-Control": "no-cache",
                    "Content-Type": "application/json",
                    "Content-Length": Buffer.byteLength(dataJSON, "utf8")
                });
                response.end(dataJSON);
                return;
            }

            var stats = replies[0];
            stats.hashrate = minerStats[address] && minerStats[address]["hashrate"] ? minerStats[address]["hashrate"] : 0;
            stats.roundScore = minerStats[address] && minerStats[address]["roundScore"] ? minerStats[address]["roundScore"] : 0;
            stats.roundHashes = minerStats[address] && minerStats[address]["roundHashes"] ? minerStats[address]["roundHashes"] : 0;
            if(replies[4]) {
                var hr_avg = extractAverageHashrates(replies[4]);
                stats.hashrate_1h  = hr_avg[0];
                stats.hashrate_6h  = hr_avg[1];
                stats.hashrate_24h = hr_avg[2];
            }

            var paymentsData = replies[1];
            var pasaPaymentsData = replies[2];
            var sharesData = JSON.parse(replies[5]);

            var workersData = [];
            for(var i=0; i<replies[3].length; i++) {
                var key = replies[3][i];
                var keyParts = key.split(":");
                var miner = keyParts[2];
                if(miner.indexOf("~") !== -1) {
                    var workerName = miner.substr(miner.indexOf("~")+1, miner.length);
                    var workerData = {
                        name: workerName,
                        hashrate: minerStats[miner] && minerStats[miner]["hashrate"] ? minerStats[miner]["hashrate"] : 0
                    };
                    workersData.push(workerData);
                }
            }

            charts.getUserChartsData(address, paymentsData, function(error, chartsData) {
                var redisCommands = [];
                for(var i in workersData) {
                    redisCommands.push(["hgetall", redisPrefix + ":unique_workers:" + address + "~" + workersData[i].name]);
                    redisCommands.push(["get", redisPrefix + ":charts:worker_hashrate:" + address + "~" + workersData[i].name]);
                }
                redisClient.multi(redisCommands).exec(function(error, replies) {
                    for(var i in workersData) {
                        var wi = 2*i;
                        var hi = wi + 1
                        if(replies[wi]) {
                            workersData[i].lastShare = replies[wi]["lastShare"] ? parseInt(replies[wi]["lastShare"]) : 0;
                            workersData[i].hashes = replies[wi]["hashes"] ? parseFloat(replies[wi]["hashes"]) : 0;
                        }
                        if(replies[hi]) {
                            var avgs = extractAverageHashrates(replies[hi]);
                            workersData[i]["hashrate_1h"]  = avgs[0];
                            workersData[i]["hashrate_6h"]  = avgs[1];
                            workersData[i]["hashrate_24h"]  = avgs[2];
                        }
                    }

                    chartsData.shares = sharesData;

                    var data = {
                        account: login_parts,
                        stats: stats,
                        payments: paymentsData,
                        pasaPayments: pasaPaymentsData,
                        charts: chartsData,
                        workers: workersData
                    }

                    var dataJSON = JSON.stringify(data);

                    response.writeHead("200", {
                        "Access-Control-Allow-Origin": "*",
                        "Cache-Control": "no-cache",
                        "Content-Type": "application/json",
                        "Content-Length": Buffer.byteLength(dataJSON, "utf8")
                    });
                    response.end(dataJSON);
                });
            });
        });
    }
}

/**
 * Return payments history
 **/
function handleGetPayments(urlParts, response) {
    var paymentKey = ":payments:all";

    if(urlParts.query.address)
        paymentKey = ":payments:" + urlParts.query.address;

    redisClient.zrevrangebyscore(
            redisPrefix + paymentKey,
            "(" + urlParts.query.time,
            "-inf",
            "WITHSCORES",
            "LIMIT",
            0,
            config.api.payments,
        function(err, result) {
            var reply;

            if(err) {
                var data = {error: "Query failed"};
            } else {
                var data = result;
            }

            reply = JSON.stringify(data);

            response.writeHead("200", {
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-cache",
                "Content-Type": "application/json",
                "Content-Length": Buffer.byteLength(reply, "utf8")
            });
            response.end(reply);
        }
    )
}

/**
 * Return pasa payments history
 **/
function handleGetPasaPayments(urlParts, response) {
    var paymentKey = ":pasaPayments:all";

    if(urlParts.query.address)
        paymentKey = ":pasaPayments:" + urlParts.query.address;

    redisClient.zrevrangebyscore(
            redisPrefix + paymentKey,
            "(" + urlParts.query.time,
            "-inf",
            "WITHSCORES",
            "LIMIT",
            0,
            config.api.payments,
        function(err, result) {
            var reply;

            if(err) {
                var data = {error: "Query failed"};
            } else {
                var data = result;
            }

            reply = JSON.stringify(data);

            response.writeHead("200", {
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-cache",
                "Content-Type": "application/json",
                "Content-Length": Buffer.byteLength(reply, "utf8")
            });
            response.end(reply);
        }
    )
}


/**
 * Return blocks data
 **/
function handleGetBlocks(urlParts, response) {
    redisClient.zrevrangebyscore(
            redisPrefix + ":blocks:matured",
            "(" + urlParts.query.height,
            "-inf",
            "WITHSCORES",
            "LIMIT",
            0,
            config.api.blocks,
        function(err, result) {

            var reply;

            if(err) {
                var data = {error: "Query failed"};
            } else {
                var data = result;
            }

            reply = JSON.stringify(data);

            response.writeHead("200", {
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-cache",
                "Content-Type": "application/json",
                "Content-Length": Buffer.byteLength(reply, "utf8")
            });
            response.end(reply);

        });
}

/**
 * Get market exchange prices
 **/
function handleGetMarket(urlParts, response) {
    response.writeHead(200, {
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json"
    });
    response.write("\n");

    // Get market prices
    market.get(function(data) {
        response.end(JSON.stringify(data));
    });
}

/**
 * Return top 25 miners
 **/
function handleTopMiners(urlParts, response) {
    var limit = parseInt(urlParts.query.limit || 25);

    if(isNaN(limit)) {
        response.end(JSON.stringify({error: "Invalid limit arg"}));
        return;
    }

    async.waterfall([
        function(callback) {
            redisClient.keys(redisPrefix + ":workers:*", callback);
        },
        function(workerKeys, callback) {
            var redisCommands = workerKeys.map(function(k) {
                return ["hmget", k, "lastShare", "hashes"];
            });
            redisClient.multi(redisCommands).exec(function(error, redisData) {
                var minersData = [];
                for(var i in redisData) {
                    var keyParts = workerKeys[i].split(":");
                    var address = keyParts[keyParts.length-1];
                    var data = redisData[i];
                    minersData.push({
                        account: utils.validateLogin(address),
                        miner: address,
                        hashrate: minersHashrate[address] && minerStats[address]["hashrate"] ? minersHashrate[address] : 0,
                        lastShare: data[0],
                        hashes: data[1]
                    });
                }
                callback(null, minersData);
            });
        }
    ], function(error, data) {
        if(error) {
            response.end(JSON.stringify({error: "Error collecting top miners stats"}));
            return;
        }

        data.sort(compareTopMiners);

        if(limit != 0) {
            data = data.slice(0, limit);
        }

        var reply = JSON.stringify(data);

        response.writeHead("200", {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "Content-Length": Buffer.byteLength(reply, "utf8")
        });
        response.end(reply);
    });
}

function compareTopMiners(a,b) {
    var v1 = a.hashrate ? parseInt(a.hashrate) : 0;
    var v2 = b.hashrate ? parseInt(b.hashrate) : 0;
    if(v1 > v2) return -1;
    if(v1 < v2) return 1;
    return 0;
}

/**
 * Miner settings
 **/

// Get current miner settings
function handleGetMinerSettings(urlParts, response) {
    response.writeHead(200, {
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json"
    });
    response.write("\n");

    var account = urlParts.query.account ? urlParts.query.account.trim() : false;

    // Check the minimal required parameters for this handle.
    if(!account) {
        response.end(JSON.stringify({status: "You must specify your email / password"}));
        return;
    }

    if(!/^[0-9a-zA-Z\-\.]+$/.test(account)) {
        response.end(JSON.stringify({status: "Invalid account"}));
        return;
    }

    var login_parts = utils.validateLogin(account);
    if(!login_parts.valid) {
        response.end(JSON.stringify({status: "Invalid account"}));
        return;
    } else {
        account = login_parts.address_pid;
    }

    // Return current miner settings
    redisClient.hgetall(redisPrefix + ":workers:" + account, function(error, values) {
        if(error || !values) {
            response.end(JSON.stringify({status: "An error occurred when fetching the value in our database"}));
            return;
        }

        if(values.hasOwnProperty("minPayoutLevel")) {
            var minPayout = parseFloat(values.minPayoutLevel);

            if(isNaN(minPayout)) {
                minPayout = config.payments.defaultPayment;
            }

            var minLevel = config.payments.minPayment;
            if(minLevel < 0) minLevel = 0;

            var maxLevel = config.payments.maxPayment ? config.payments.maxPayment : null;

            if(minPayout < minLevel) {
                minPayout = minLevel;
            }

            if(maxLevel && minPayout > maxLevel) {
                minPayout = maxLevel;
            }
        } else {
            var minPayout = config.payments.defaultPayment;
        }

        if(values.hasOwnProperty("publicKey")) {
            var publicKey = values.publicKey;
        } else {
            var publicKey = false;
        }

        if(values.hasOwnProperty("notify")) {
            var notify = values.notify;
        } else {
            var notify = config.email.defaultNotifications;
        }

        response.end(JSON.stringify({
            status: "done",
            minPayout: minPayout,
            publicKey: publicKey,
            notify: notify
        }));
    });
}

// Set miner settings
function handleSetMinerSettings(urlParts, response) {
    response.writeHead(200, {
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json"
    });
    response.write("\n");

    var account = urlParts.query.account ? urlParts.query.account.trim() : false;
    var password = urlParts.query.email ? urlParts.query.email.trim() : false;
    var minPayout = urlParts.query.minPayout ? urlParts.query.minPayout.trim() : false;
    var publicKey = urlParts.query.publicKey ? urlParts.query.publicKey.trim() : false;
    if(Array.isArray(urlParts.query["notify[]"])) {
        var notify = urlParts.query["notify[]"].join(",");
    } else {
        var notify = urlParts.query["notify[]"] ? urlParts.query["notify[]"].trim() : "";
    }

    // Check the minimal required parameters for this handle.
    if(!account || !password) {
        response.end(JSON.stringify({status: "You must specify your email / password"}));
        return;
    }

    if(!/^[0-9a-zA-Z\-\.]+$/.test(account)) {
        response.end(JSON.stringify({status: "Invalid account"}));
        return;
    }

    var login_parts = utils.validateLogin(account);
    if(!login_parts.valid) {
        response.end(JSON.stringify({status: "Invalid account"}));
        return;
    } else {
        account = login_parts.address_pid;
    }

    if(password.trim() == "") {
        response.end(JSON.stringify({status: "Empty email / password"}));
        return;
    }

    if(password.indexOf("*") !== -1) {
        response.end(JSON.stringify({status: "Invalid characters in email / password"}));
        return;
    }

    if(minPayout) {
        minPayout = parseFloat(minPayout);
        if(isNaN(minPayout)) {
            response.end(JSON.stringify({status: "Your minimum payout level doesn't look like a number"}));
            return;
        }
        var minLevel = config.payments.minPayment;
        if(minLevel < 0) minLevel = 0;

        var maxLevel = config.payments.maxPayment ? config.payments.maxPayment : null;

        if(minPayout < minLevel) {
            response.end(JSON.stringify({status: "The minimum payout level is " + minLevel}));
            return;
        }

        if(maxLevel && minPayout > maxLevel) {
            response.end(JSON.stringify({status: "The maximum payout level is " + maxLevel}));
            return;
        }
    }

    if(publicKey) {
        if(!/^[a-zA-Z0-9]+$/.test(publicKey)) {
            response.end(JSON.stringify({status: "Public key contains invalid characters"}));
            return;
        }
        var decoded = base58.decode(publicKey).toString("hex");
        if(decoded.length != 150) {
            response.end(JSON.stringify({status: "Invalid public key length"}));
            return;
        }
    }

    if(notify != "" && !/^[a-zA-Z0-9\-_,]+$/.test(notify)) {
        response.end(JSON.stringify({status: "Invalid notify settings"}));
        return;
    }

    // Only do a modification if we have seen the password in combination with the wallet address.
    minerSeenWithPasswordForAddress(account, password, function(error, found) {
        if(!found || error) {
          response.end(JSON.stringify({status: error}));
          return;
        }

        var redisCommands = [];

        if(minPayout) {
            redisCommands.push(
	        ["hset", redisPrefix + ":workers:" + account, "minPayoutLevel", minPayout]
            );
        }
        if(publicKey) {
            redisCommands.push(
	        ["hset", redisPrefix + ":workers:" + account, "publicKey", publicKey]
            );
        }
        redisCommands.push(
	    ["hset", redisPrefix + ":workers:" + account, "notify", notify]
        );

        redisClient.multi(redisCommands).exec(function(error, replies) {
            if(error) {
                response.end(JSON.stringify({status: "An error occurred when updating the value in our database"}));
                return;
	    }
            response.end(JSON.stringify({status: "done"}));
        });
    });
}

/**
 * Return miners hashrate
 **/
function handleGetMinersHashrate(response) {
    var data = {};
    for(var miner in minersHashrate) {
        if(miner.indexOf("~") !== -1) continue;
        data[miner] = minersHashrate[miner];
    }

    data = {
        minersHashrate: data
    }

    var reply = JSON.stringify(data);

    response.writeHead("200", {
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(reply, "utf8")
    });
    response.end(reply);
}

/**
 * Return workers hashrate
 **/
function handleGetWorkersHashrate(response) {
    var data = {};
    for(var miner in minersHashrate) {
        if(miner.indexOf("~") === -1) continue;
        data[miner] = minersHashrate[miner];
    }

    var reply = JSON.stringify({
        workersHashrate: data
    });

    response.writeHead("200", {
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "Content-Length": reply.length
    });
    response.end(reply);
}


/**
 * Authorize access to a secured API call
 **/
function authorize(request, response) {
    var sentPass = url.parse(request.url, true).query.password;

    var remoteAddress = request.connection.remoteAddress;
    if(config.api.trustProxyIP && request.headers["x-forwarded-for"]) {
      remoteAddress = request.headers["x-forwarded-for"];
    }

    var bindIp = config.api.bindIp ? config.api.bindIp : "0.0.0.0";
    if(typeof sentPass == "undefined" && (remoteAddress === "127.0.0.1" || remoteAddress === "::ffff:127.0.0.1" || remoteAddress === "::1" || (bindIp != "0.0.0.0" && remoteAddress === bindIp))) {
        return true;
    }

    response.setHeader("Access-Control-Allow-Origin", "*");

    var cookies = parseCookies(request);
    if(typeof sentPass == "undefined" && cookies.sid && cookies.sid === authSid) {
        return true;
    }

    if(sentPass !== config.api.password) {
        response.statusCode = 401;
        response.end("Invalid password");
        return;
    }

    log("warn", logSystem, "Admin authorized from %s", [remoteAddress]);
    response.statusCode = 200;

    var cookieExpire = new Date( new Date().getTime() + 60*60*24*1000);
    response.setHeader("Set-Cookie", "sid=" + authSid + "; path=/; expires=" + cookieExpire.toUTCString());
    response.setHeader("Cache-Control", "no-cache");
    response.setHeader("Content-Type", "application/json");

    return true;
}

/**
 * Administration: return pool statistics
 **/
function handleAdminStats(response) {
    async.waterfall([

        //Get worker keys & unlocked blocks
        function(callback) {
            redisClient.multi([
                ["keys", redisPrefix + ":workers:*"],
                ["zrange", redisPrefix + ":blocks:matured", 0, -1]
            ]).exec(function(error, replies) {
                if(error) {
                    log("error", logSystem, "Error trying to get admin data from redis %j", [error]);
                    callback(true);
                    return;
                }
                callback(null, replies[0], replies[1]);
            });
        },

        //Get worker balances
        function(workerKeys, blocks, callback) {
            var redisCommands = workerKeys.map(function(k) {
                return ["hmget", k, "balance", "paid"];
            });
            redisClient.multi(redisCommands).exec(function(error, replies) {
                if(error) {
                    log("error", logSystem, "Error with getting balances from redis %j", [error]);
                    callback(true);
                    return;
                }

                callback(null, replies, blocks);
            });
        },
        function(workerData, blocks, callback) {
            var stats = {
                totalOwed: 0,
                totalPaid: 0,
                totalRevenue: 0,
                totalDiff: 0,
                totalShares: 0,
                blocksOrphaned: 0,
                blocksUnlocked: 0,
                totalWorkers: 0
            };

            for(var i = 0; i < workerData.length; i++) {
                stats.totalOwed += parseInt(workerData[i][0]) || 0;
                stats.totalPaid += parseInt(workerData[i][1]) || 0;
                stats.totalWorkers++;
            }

            for(var i = 0; i < blocks.length; i++) {
                var block = blocks[i].split(":");
                if(block[6] == 1) {
                    stats.blocksOrphaned++;
                } else {
                    stats.blocksUnlocked++;
                    stats.totalDiff += parseFloat(block[3]);
                    stats.totalShares += parseInt(block[4]);
                    stats.totalRevenue += parseInt(block[5]);
                }
            }
            callback(null, stats);
        }
    ], function(error, stats) {
            if(error) {
                response.end(JSON.stringify({error: "Error collecting stats"}));
                return;
            }
            response.end(JSON.stringify(stats));
        }
    );

}

/**
 * Administration: users list
 **/
function handleAdminUsers(response) {
    async.waterfall([
        // get workers Redis keys
        function(callback) {
            redisClient.keys(redisPrefix + ":workers:*", callback);
        },
        // get workers data
        function(workerKeys, callback) {
            var redisCommands = workerKeys.map(function(k) {
                return ["hmget", k, "lockedBalance", "balance", "paid", "lastShare", "hashes"];
            });
            redisClient.multi(redisCommands).exec(function(error, redisData) {
                var workersData = {};
                for(var i in redisData) {
                    var keyParts = workerKeys[i].split(":");
                    var address = keyParts[keyParts.length-1];
                    var data = redisData[i];
                    workersData[address] = {
                        unconfirmed: data[0],
                        balance: data[1],
                        paid: data[2],
                        lastShare: data[3],
                        hashes: data[4],
                        hashrate: minerStats[address] && minerStats[address]["hashrate"] ? minerStats[address]["hashrate"] : 0,
                        roundScore: minerStats[address] && minerStats[address]["roundScore"] ? minerStats[address]["roundScore"] : 0,
                        roundHashes: minerStats[address] && minerStats[address]["roundHashes"] ? minerStats[address]["roundHashes"] : 0
                    };
                }
                callback(null, workersData);
            });
        }
        ], function(error, workersData) {
            if(error) {
                response.end(JSON.stringify({error: "Error collecting users stats"}));
                return;
            }
            response.end(JSON.stringify(workersData));
        }
    );
}

/**
 * Administration: pool monitoring
 **/
function handleAdminMonitoring(response) {
    response.writeHead("200", {
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json"
    });
    async.parallel({
        monitoring: getMonitoringData,
        logs: getLogFiles
    }, function(error, result) {
        response.end(JSON.stringify(result));
    });
}

/**
 * Administration: log file data
 **/
function handleAdminLog(urlParts, response) {
    var file = urlParts.query.file;
    var filePath = config.logging.files.directory + "/" + file;
    if(!file.match(/^\w+\.log$/)) {
        response.end("wrong log file");
    }
    response.writeHead(200, {
        "Content-Type": "text/plain",
        "Cache-Control": "no-cache",
        "Content-Length": fs.statSync(filePath).size
    });
    fs.createReadStream(filePath).pipe(response);
}

/**
 * Administration: pool ports usage
 **/
function handleAdminPorts(response) {
    async.waterfall([
        function(callback) {
            redisClient.keys(redisPrefix + ":ports:*", callback);
        },
        function(portsKeys, callback) {
            var redisCommands = portsKeys.map(function(k) {
                return ["hmget", k, "port", "users"];
            });
            redisClient.multi(redisCommands).exec(function(error, redisData) {
                var portsData = {};
                for(var i in redisData) {
                    var port = portsKeys[i];

                    var data = redisData[i];
                    portsData[port] = {
                        port: data[0],
                        users: data[1]
                    };
                }
                callback(null, portsData);
            });
        }
    ], function(error, portsData) {
        if(error) {
            response.end(JSON.stringify({error: "Error collecting Ports stats"}));
            return;
        }
        response.end(JSON.stringify(portsData));
    });
}

/**
 * Administration: test email notification
 **/
function handleTestEmailNotification(urlParts, response) {
    var email = urlParts.query.email;
    if(!config.email) {
        response.end(JSON.stringify({status: "Email system is not configured"}));
        return;
    }
    if(!config.email.enabled) {
        response.end(JSON.stringify({status: "Email system is not enabled"}));
        return;
    }
    if(!email) {
        response.end(JSON.stringify({status: "No email specified"}));
        return;
    }
    log("info", logSystem, "Sending test e-mail notification to %s", [email]);
    notifications.sendToEmail(email, "test_notification", {});
    response.end(JSON.stringify({status: "done"}));
}

// Initialize monitoring
function initMonitoring() {
    if(!config.daemon.monitoring.enabled) {
        return;
    }
    var interval = config.daemon.monitoring.interval;
    setInterval(function() {
        daemonRpc.async("getwalletcoins")
            .then((data) => {
                var stat = {
                    lastCheck: new Date() / 1000 | 0
                };
                if(data.hasOwnProperty("error")) {
                    stat.lastResponse = JSON.stringify(data);
                    stat.lastFail = stat.lastCheck;
                    stat.lastFailResponse = stat.lastResponse;
                    stat.lastStatus = "fail";
                } else {
                    stat.lastResponse = JSON.stringify(data);
                    stat.lastStatus = "ok";
                }
                var redisCommands = [];
                for(var property in stat) {
                    redisCommands.push(["hset", redisPrefix + ":status:daemon", property, stat[property]]);
                }
                redisClient.multi(redisCommands).exec();
            });
    }, interval * 1000);
}

// Get monitoring data
function getMonitoringData(callback) {
    redisClient.hgetall(redisPrefix + ":status:daemon", function(error, results) {
        callback(error, results);
    });
}

/**
 * Return pool public ports
 **/
function getPublicPorts(ports, callback) {
    async.waterfall([
        function(callback) {
            redisClient.keys(redisPrefix + ":ports:*", callback);
        },
        function(portsKeys, callback2) {
            var redisCommands = portsKeys.map(function(k) {
                return ["hmget", k, "port", "users"];
            });
            redisClient.multi(redisCommands).exec(function(error, redisData) {
                var portsData = {};
                for(var i in redisData) {
                    var data = redisData[i];
                    portsData[data[0]] = data[1];
                }
                callback2(null, portsData);
            });
        }
    ], function(error, portsData) {
        var filterPorts = ports.filter(function(port) {
            if(portsData[port.port]) {
                port.users = portsData[port.port];
            }
            return !port.hidden;
        });
        callback(filterPorts);
    });
}

/**
 * Return list of pool logs file
 **/
function getLogFiles(callback) {
    var dir = config.logging.files.directory;
    fs.readdir(dir, function(error, files) {
        var logs = {};
        for(var i in files) {
            var file = files[i];
            if(!file.endsWith(".log")) continue;
            var stats = fs.statSync(dir + "/" + file);
            logs[file] = {
                size: stats.size,
                changed: Date.parse(stats.mtime) / 1000 | 0
            }
        }
        callback(error, logs);
    });
}

/**
 * Check if a miner has been seen with specified password
 **/
function minerSeenWithPasswordForAddress(account, password, callback) {
    redisClient.zrevrangebyscore(
        redisPrefix + ":worker_passwords:" + account,
        "+inf",
        "(" + (Math.floor(Date.now() / 1000) - (config.poolServer.passwordExpireTime | 3600)),
        function(error, result) {
            if(!Array.isArray(result)) {
                callback("Error checking password, you may not have submitted a share recently", false);
                return;
            }
            if(result.length === 0) {
                callback("Your password has expired, please submit at least one share with a new password", false);
                return;
            }
            if(result.length !== 1) {
                callback("You are not using the same password for all workers", false);
                return;
            }
            if(result[0] !== password) {
                callback("Invalid password", false);
                return;
            }
            callback(false, true);
        }
    );
}

/**
 * Parse cookies data
 **/
function parseCookies(request) {
    var list = {},
        rc = request.headers.cookie;
    rc && rc.split(";").forEach(function(cookie) {
        var parts = cookie.split("=");
        list[parts.shift().trim()] = unescape(parts.join("="));
    });
    return list;
}
/**
 * Start pool API
 **/

// Collect statistics for the first time
collectStats();

// Initialize RPC monitoring
initMonitoring();

// Enable to be bind to a certain ip or all by default
var bindIp = config.api.bindIp ? config.api.bindIp : "0.0.0.0";

// Start API on HTTP port
var server = http.createServer(function(request, response) {
    if(request.method.toUpperCase() === "OPTIONS") {
        response.writeHead("204", "No Content", {
            "access-control-allow-origin": "*",
            "access-control-allow-methods": "GET, POST, PUT, DELETE, OPTIONS",
            "access-control-allow-headers": "content-type, accept",
            "access-control-max-age": 10, // Seconds.
            "content-length": 0
        });
        return(response.end());
    }

    handleServerRequest(request, response);
});

server.listen(config.api.port, bindIp, function() {
    log("info", logSystem, "API started & listening on %s port %d", [bindIp, config.api.port]);
});
