/**
 * PascalCoin Open Pool
 * https://github.com/PascalCoinPool/pascalcoin-open-pool
 *
 * Share Processor
 *
 * This module deals with handling shares when in internal payment processing mode.
 * It connects to redis and inserts shares and stats into the database.
 *
 **/

// Initialize log system
var logSystem = "shareProcessor";
require("./exceptionWriter.js")(logSystem);

module.exports = function() {

    var redisCfg = config.redis;
    var prefix = redisCfg.prefix;

    var cleanupInterval = redisCfg.cleanupInterval && redisCfg.cleanupInterval > 0 ? redisCfg.cleanupInterval : 15;

    /**
     * Record miner share data
     **/
    this.handleShare = function(isValidShare, isValidBlock, shareData) {

	var dateNow = Date.now();
	var dateNowSeconds = dateNow / 1000 | 0;
        var dateNowHour = Math.floor(dateNowSeconds / 3600) * 3600;

	var height = shareData.height;
	var worker = shareData.worker;
	var workerName = shareData.workerName;
	var workerPass = shareData.workerPass;

	/**
	 * The difficulty of the share multiplied by 2^32 for the number of
         * total hashes expected to create a share of this difficulty
	 **/
	var difficulty = Math.round(Math.pow(2, 32) * shareData.difficulty);

	var redisCommands = [];

        redisCommands.push(
            ["get", prefix + ":charts:shares:" + worker]
        );

        if(isValidShare) {
            redisCommands.push(
		["hincrby", prefix + ":shares:roundCurrent", worker, difficulty],
		["hincrby", prefix + ":workers:" + worker, "validShares", 1]
	    );
        } else {
            redisCommands.push(
		["hincrby", prefix + ":workers:" + worker, "invalidShares", 1]
	    );
        }

	redisCommands.push(
	    ["zadd", prefix + ":hashrate", dateNowSeconds, [difficulty, worker, dateNow].join(":")],
	    ["hincrby", prefix + ":workers:" + worker, "hashes", difficulty],
	    ["hset", prefix + ":workers:" + worker, "lastShare", dateNowSeconds],
	    ["expire", prefix + ":workers:" + worker, (86400 * cleanupInterval)],
	    ["expire", prefix + ":payments:" + worker, (86400 * cleanupInterval)]
	);

	if(workerName) {
	    redisCommands.push(
		["zadd", prefix + ":hashrate", dateNowSeconds, [difficulty, worker + "~" + workerName, dateNow].join(":")],
		["hincrby", prefix + ":unique_workers:" + worker + "~" + workerName, "hashes", difficulty],
		["hset", prefix + ":unique_workers:" + worker + "~" + workerName, "lastShare", dateNowSeconds],
		["expire", prefix + ":unique_workers:" + worker + "~" + workerName, (86400 * cleanupInterval)]
	    );
	}

        if(workerPass) {
            redisCommands.push(
	        ["zadd", prefix + ":worker_passwords:" + worker, dateNowSeconds, workerPass]
            );
        } else {
            // Nanominer does not send password with share, so we must update the score for all existing valid passwords
            redisClient.zrevrangebyscore(
                redisPrefix + ":worker_passwords:" + worker,
                "+inf",
                "(" + (Math.floor(Date.now() / 1000) - (config.poolServer.passwordExpireTime | 3600)),
                function(error, result) {
                    if(!Array.isArray(result) || result.length == 0) {
                        return;
                    }
                    var workerCommands = [];
                    for(var i = 0; i < result.length; i++) {
                        workerCommands.push(
	                    ["zadd", prefix + ":worker_passwords:" + worker, dateNowSeconds, result[i]]
                        );
                    }
                    redisClient.multi(workerCommands).exec(function(err, replies) {
                        if(err) {
		            log("warn", logSystem, "Error updating password for %s", [worker]);
                        }
                    });
                }
            );

        }

        if(isValidBlock) {
	    redisCommands.push(
		["hset", prefix + ":stats", "lastBlockFound", Date.now()],
		["renamenx", prefix + ":shares:roundCurrent", prefix + ":shares:round" + height],
		["hgetall", prefix + ":shares:round" + height],
	    );
	}

        redisClient.multi(redisCommands).exec(function(err, replies) {
            if(err) {
		log("error", logSystem, "Error with share processor multi %s", [JSON.stringify(err)]);
	    }

	    if(isValidBlock) {
		var workerShares = replies[replies.length - 1];
		var totalShares = Object.keys(workerShares).reduce(function(p, c){
		    return p + parseInt(workerShares[c])
		}, 0);
		redisClient.zadd(prefix + ":blocks:candidates", height, [
		    worker,
		    shareData.blockHash,
		    Date.now() / 1000 | 0,
		    Math.round(Math.pow(2, 32) * shareData.blockDiff),
		    totalShares
		].join(":"), function(err, result){
		    if (err){
			log("error", logSystem, "Failed inserting block candidate %s \n %j", [hashHex, err]);
		    }
		});

	    }

            // add share to chart data
            var shares = replies[0];
            if(shares == null) {
                shares = [];
            } else {
                shares = JSON.parse(shares);
            }

            if(shares.length > 0 && shares[shares.length-1][0] == dateNowHour) {
                shares[shares.length-1][1] += difficulty;
                shares[shares.length-1][2]++;
            } else {
                shares.push([dateNowHour, difficulty, 1]);
            }
            while(dateNowSeconds - shares[0][0] > config.charts.user.hashrate.maximumPeriod) {
                shares.shift();
            }
            redisClient.set(prefix + ":charts:shares:" + worker, JSON.stringify(shares), "EX", config.charts.user.hashrate.maximumPeriod);

        });

    };

};
