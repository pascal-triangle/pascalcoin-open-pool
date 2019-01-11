/**
 * PascalCoin Open Pool
 * https://github.com/PascalCoinPool/pascalcoin-open-pool
 *
 * Block unlocker
 **/

// Load required modules
var async = require("async");

var utils = require("./utils.js");

// get daemon RPC instance
var daemon = require("./daemon.js");
var daemonRpc = new daemon.Rpc();


/**
 * Initialize log system
 **/
var logSystem = "unlocker";
require("./exceptionWriter.js")(logSystem);


/**
 * Get all block candidates in redis
 **/
function getBlockCandidates(callback) {
    redisClient.zrange(redisPrefix + ":blocks:candidates", 0, -1, "WITHSCORES", function(error, results) {
        if(error) {
            log("error", logSystem, "Error trying to get pending blocks from redis %j", [error]);
            callback(true);
            return;
        }
        if(results.length === 0) {
            log("info", logSystem, "No blocks candidates in redis");
            callback(true);
            return;
        }

        var blocks = [];

        for (var i = 0; i < results.length; i += 2) {
            var parts = results[i].split(":");
            blocks.push({
                parts: parts,
                serialized: results[i],
                height: parseInt(results[i + 1]),
                worker: parts[0],
                hash: parts[1],
                time: parts[2],
                difficulty: parts[3],
                shares: parts[4]
            });
        }

        callback(null, blocks);
    });
}

/**
 * Get block from RPC to check if orphaned, unlocked, and reward amount
 **/
function getBlockRpc(blocks, callback) {

    async.eachLimit(blocks, 10, function(block, callback2) {

        daemonRpc.async("getblock", {"block":block.height})
            .then((data) => {

                if(data.hasOwnProperty("error")) {
                    log("error", logSystem, "Error with getblock RPC request for block %s - %s",
                        [block.height, JSON.stringify(data.error)]);
                    block.unlocked = false;
                    callback2(data.error.message);
                    return;
                }

                block.mature = data.result.maturation < 2 ? 0 : 1;
                block.orphaned = data.result.pow.toUpperCase() === block.hash.toUpperCase() ? 0 : 1;
                block.unlocked = data.result.maturation >= config.blockUnlocker.depth;
                block.pasaReward = Math.floor(parseInt(data.result.reward*10000) - parseInt(config.fees.devFee*10000));
                block.reward = Math.floor(parseInt(data.result.reward*10000) + parseInt(data.result.fee*10000) - parseInt(config.fees.devFee*10000));

                callback2();
            });

    }, function(err) {
        if(err) {
            log("error", logSystem, "Stopping block unlocker");
            callback(true);
            return;
        }
        callback(null, blocks)
    })
}

/**
 * Get worker shares for each unlocked block
 **/
function getShares(blocks, callback) {

    var redisCommands = blocks.map(function(block) {
        return ["hgetall", redisPrefix + ":shares:round" + block.height];
    });

    redisClient.multi(redisCommands).exec(function(error, replies) {
        if(error) {
            log("error", logSystem, "Error with getting round shares from redis %j", [error]);
            callback(true);
            return;
        }

        for (var i = 0; i < replies.length; i++) {
            var workerScores = replies[i];
            blocks[i].workerScores = workerScores;
            blocks[i].workerRewards = {};
            blocks[i].workerPasaRewards = {};
            var totalScore = parseInt(blocks[i].shares);
            var reward = Math.floor((blocks[i].reward - blocks[i].reward * config.fees.poolFee / 100));
            var pasaReward = Math.floor((blocks[i].pasaReward - blocks[i].pasaReward * config.fees.poolFee / 100));

            // In case you want to reserve a few molinas for transferring, but it is not needed
            //reward -= 6; // reserve 0.0006 PASC

            if(workerScores) {
                Object.keys(workerScores).forEach(function(worker) {
                    var percent = workerScores[worker] / totalScore;
                    var workerReward = Math.floor(reward * percent);
                    blocks[i].workerRewards[worker] = [workerReward, percent];
                    var workerPasaReward = Math.floor(pasaReward * percent);
                    blocks[i].workerPasaRewards[worker] = [workerPasaReward, percent];
                });
            }

        }

        callback(null, blocks);
    });
}


/**
 * Handle pending blocks and increase locked balance
 **/
function handlePending(blocks, callback) {
    var pendingBlocksCommands = [];
    var lockedBalances = {};
    var totalBlocksPending = 0;
    blocks.forEach(function(block) {
        if(!block.mature) return;

        totalBlocksPending++;

        // If we only have five parts of the block, then
        // fill in the rest of the information from the
        // RPC command and increase pending balance.
        if(block.parts.length == 5) {
            block.parts = [
                block.worker,
                block.hash,
                block.time,
                block.difficulty,
                block.shares,
                block.reward,
                block.orphaned,
            ];
            pendingBlocksCommands.push(["zrem", redisPrefix + ":blocks:candidates", block.serialized]);
            block.serialized = block.parts.join(":");
            pendingBlocksCommands.push(["zadd", redisPrefix + ":blocks:candidates", block.height, block.serialized]);

            log("info", logSystem, "Pending block %d with reward %d", [block.height, (block.reward/10000).toFixed(4)]);

            Object.keys(block.workerRewards).forEach(function(worker) {
                lockedBalances[worker] = (lockedBalances[worker] || 0) + block.workerRewards[worker][0];
                log("info", logSystem, "Block %d pending balance to %s for %d%% of total block score: %d",
                    [block.height, worker, block.workerRewards[worker][1]*100,
                     (block.workerRewards[worker][0]/10000).toFixed(4)]);
            });

        }

    });

    for (var worker in lockedBalances) {
        var amount = lockedBalances[worker];
        if(amount <= 0) {
            delete lockedBalances[worker];
            continue;
        }
        pendingBlocksCommands.push(["hincrby", redisPrefix + ":workers:" + worker, "lockedBalance", amount]);
    }

    if(pendingBlocksCommands.length > 0) {
        redisClient.multi(pendingBlocksCommands).exec(function(error, replies) {
            if(error) {
                log("error", logSystem, "Error with pending blocks %j", [error]);
                callback(true);
                return;
            }
            log("info", logSystem, "Pending %d blocks and update balances for %d workers",
                [totalBlocksPending, Object.keys(lockedBalances).length]);
            callback(null, blocks);
        });
    } else {
        callback(null, blocks);
    }
}


/**
 * Handle orphaned blocks
 **/
function handleOrphan(blocks, callback) {
    var orphanCommands = [];
    var orphanedBalances = {};
    var totalBlocksOrphaned = 0;
    blocks.forEach(function(block) {
        if(!block.orphaned || !block.mature) return;

        totalBlocksOrphaned++;

        //orphanCommands.push(["del", redisPrefix + ":shares:round" + block.height]);
        orphanCommands.push(["zrem", redisPrefix + ":blocks:candidates", block.serialized]);
        orphanCommands.push(["zadd", redisPrefix + ":blocks:matured", block.height, [
            block.worker,
            block.hash,
            block.time,
            block.difficulty,
            block.shares,
            0,
            block.orphaned
        ].join(":")]);

        // Add orphaned shares to current block
        /*
        if(block.workerScores) {
            var workerScores = block.workerScores;
            Object.keys(workerScores).forEach(function (worker) {
                orphanCommands.push(["hincrby", redisPrefix + ":shares:roundCurrent", worker, workerScores[worker]]);
            });
        }
        */

        log("info", logSystem, "Orphaned block %d with reward %d", [block.height, (block.reward/10000).toFixed(4)]);

        Object.keys(block.workerRewards).forEach(function(worker) {
            orphanedBalances[worker] = (orphanedBalances[worker] || 0) + block.workerRewards[worker][0];
            log("info", logSystem, "Block %d removed balance from %s for %d%% of total block score: %d",
                [block.height, worker, block.workerRewards[worker][1]*100,
                 (block.workerRewards[worker][0]/10000).toFixed(4)]);
        });

    });

    for(var worker in orphanedBalances) {
        var amount = orphanedBalances[worker];
        if(amount <= 0) {
            delete orphanedBalances[worker];
            continue;
        }
        orphanCommands.push(["hincrby", redisPrefix + ":workers:" + worker, "lockedBalance", -amount]);
    }

    if(orphanCommands.length > 0) {
        redisClient.multi(orphanCommands).exec(function(error, replies) {
            if(error) {
                log("error", logSystem, "Error with cleaning up data in redis for orphan block(s) %j", [error]);
                callback(true);
                return;
            }
            log("info", logSystem, "Orphaned %d blocks and update balances for %d workers",
                [totalBlocksOrphaned, Object.keys(orphanedBalances).length]);
            callback(null, blocks);
        });
    } else {
        callback(null, blocks);
    }
}


/**
 * Handle unlocked blocks
 **/
function handleUnlocked(blocks, callback) {
    var unlockedBlocksCommands = [];
    var unlockedBalances = {};
    var unlockedPasaBalances = {};
    var totalBlocksUnlocked = 0;
    blocks.forEach(function(block) {
        if(block.orphaned || !block.unlocked || !block.mature) return;

        totalBlocksUnlocked++;

        unlockedBlocksCommands.push(["del", redisPrefix + ":shares:round" + block.height]);
        unlockedBlocksCommands.push(["zrem", redisPrefix + ":blocks:candidates", block.serialized]);
        unlockedBlocksCommands.push(["zadd", redisPrefix + ":blocks:matured", block.height, [
            block.worker,
            block.hash,
            block.time,
            block.difficulty,
            block.shares,
            block.reward,
            block.orphaned == 0 ? 2 : 1
        ].join(":")]);

        log("info", logSystem, "Unlocked block %d with reward %d", [block.height, (block.reward/10000).toFixed(4)]);

        Object.keys(block.workerRewards).forEach(function(worker) {
            unlockedBalances[worker] = (unlockedBalances[worker] || 0) + block.workerRewards[worker][0];
            log("info", logSystem, "Block %d payment to %s for %d%% for total payment: %d",
                [block.height, worker, block.workerRewards[worker][1]*100,
                 (block.workerRewards[worker][0]/10000).toFixed(4)]);
        });

        Object.keys(block.workerPasaRewards).forEach(function(worker) {
            unlockedPasaBalances[worker] = (unlockedPasaBalances[worker] || 0) + block.workerPasaRewards[worker][0];
            log("info", logSystem, "Block %d PASA payment to %s for %d%% for total payment: %d",
                [block.height, worker, block.workerPasaRewards[worker][1]*100,
                 (block.workerPasaRewards[worker][0]/10000).toFixed(4)]);
        });

    });

    for(var worker in unlockedBalances) {
        var amount = unlockedBalances[worker];
        if(amount <= 0) {
            delete unlockedBalances[worker];
            continue;
        }
        unlockedBlocksCommands.push(["hincrby", redisPrefix + ":workers:" + worker, "balance", amount]);
        unlockedBlocksCommands.push(["hincrby", redisPrefix + ":workers:" + worker, "lockedBalance", -amount]);
    }
    for(var worker in unlockedPasaBalances) {
        var amount = unlockedPasaBalances[worker];
        if(amount <= 0) {
            delete unlockedPasaBalances[worker];
            continue;
        }
        unlockedBlocksCommands.push(["hincrby", redisPrefix + ":workers:" + worker, "pasaBalance", amount]);
    }

    if(unlockedBlocksCommands.length > 0) {
        redisClient.multi(unlockedBlocksCommands).exec(function(error, replies) {
            if(error) {
                log("error", logSystem, "Error with unlocking blocks %j", [error]);
                callback(true);
                return;
            }
            log("info", logSystem, "Unlocked %d blocks and update balances for %d workers",
                [totalBlocksUnlocked, Object.keys(unlockedBalances).length]);
            callback(null);
        });
    } else {
        callback(null);
    }
}


/**
 * Run block unlocker
 **/
log("info", logSystem, "Started");
function runInterval() {
    async.waterfall([
        getBlockCandidates,
        getBlockRpc,
        getShares,
        handlePending,
        handleOrphan,
        handleUnlocked
    ], function(error, result) {
        setTimeout(runInterval, config.blockUnlocker.interval * 1000);
    })
}

runInterval();
