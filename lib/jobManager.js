var EventEmitter = require("events").EventEmitter;
var crypto = require("crypto");
var bignum = require("bignum");
var randomhash = require("node-randomhash");
var utils = require("./utils.js");

/**
 * Block template
 **/
var BlockTemplate = function(jobId, rpcData) {
    this.submits = [];
    this.rpcData = rpcData;
    this.jobId = jobId;
    this.target = bignum(rpcData.target_pow, 16);
    this.difficulty = parseFloat((diff1 / this.target.toNumber()).toFixed(9));

    this.registerSubmit = function(extraNonce1, extraNonce2, nTime, nonce) {
	var submission = extraNonce1 + extraNonce2 + nTime + nonce;
	if(this.submits.indexOf(submission) === -1) {
            this.submits.push(submission);
            return true;
	}
	return false;
    }

    this.getJobParams = function() {
	if(!this.jobParams) {
            this.jobParams = [
		this.jobId,
		"0000000000000000000000000000000000000000000000000000000000000000",
		this.rpcData.part1,
		this.rpcData.part3,
		[],
		"00000000",
		"10000000",
		utils.packUInt32BE(this.rpcData.timestamp).toString("hex"),
		true
            ];
	}
	return this.jobParams;
    }
}

/**
 * Unique extranonce per subscriber
 **/
var ExtraNonceCounter = function(configPoolId) {
    this.next = function() {
	var str = (configPoolId+"/"+crypto.randomBytes(4).readUInt32LE(0)).padEnd(26, "0");
	var hex = Buffer.from(str, "utf8").toString("hex");
	return hex;
    }
    this.size = 18; //bytes
}

/**
 * Unique job per new block template
 **/
var JobCounter = function() {
    var counter = 0;
    this.next = function() {
        counter++;
        if(counter % 0xffff === 0)
            counter = 1;
        return this.cur();
    }
    this.cur = function () {
        return counter.toString(16);
    }
}

/**
 * JobManager Class
 *
 * Emits:
 * - newBlock(blockTemplate)
 * - updatedBlock(blockTemplate)
 * - share(shareData, block)
 **/
var JobManager = module.exports = function JobManager() {

    // Private members

    var _this = this;
    var jobCounter = new JobCounter();

    // Public members

    this.extraNonceCounter = new ExtraNonceCounter(config.poolId);
    this.extraNoncePlaceholder = new Buffer("46726565706f6f6c2f3030303030303030303030303030303030", "hex");
    this.extraNonce2Size = this.extraNoncePlaceholder.length - this.extraNonceCounter.size;

    this.currentJob = false;
    this.validJobs = {};


    this.updateCurrentJob = function() {
        this.processTemplate(_this.currentJob.rpcData);
    };

    this.processTemplate = function(rpcData) {

        var tmpBlockTemplate = new BlockTemplate(
            jobCounter.next(),
            rpcData
        );

	if(!_this.currentJob || _this.currentJob.rpcData.block != rpcData.block) {
	    // We have a new block template
            _this.currentJob = tmpBlockTemplate;
            _this.validJobs = {};
            _this.emit("newBlock", tmpBlockTemplate);
            _this.validJobs[tmpBlockTemplate.jobId] = tmpBlockTemplate;

	} else {
            _this.currentJob = tmpBlockTemplate;
            _this.emit("updatedBlock", tmpBlockTemplate);
            _this.validJobs[tmpBlockTemplate.jobId] = tmpBlockTemplate;
	}

        return true;
    };

    this.processShare = function(jobId, previousDifficulty, difficulty, extraNonce1, extraNonce2,
				 nTime, nonce, ipAddress, port, worker, callback) {
        var shareError = function(error) {
            _this.emit("share", {
                job: jobId,
                ip: ipAddress,
                worker: worker.address_pid,
                workerName: worker.worker_id,
                workerPass: worker.password,
                difficulty: difficulty,
                shareDiff: difficulty,
                error: error[1]
            }, false);
            callback({error: error, result: null});
            return {error: error, result: null};
        };

        var submitTime = Date.now() / 1000 | 0;

        if(extraNonce2.length / 2 !== _this.extraNonce2Size)
            return shareError([20, "incorrect size of extranonce2"]);

        var job = this.validJobs[jobId];

        if(typeof job === "undefined" || job.jobId != jobId ) {
            return shareError([21, "job not found"]);
        }

        if(nTime.length !== 8) {
            return shareError([20, "incorrect size of ntime"]);
        }

        var nTimeInt = parseInt(nTime, 16);
        if(nTimeInt < job.rpcData.curtime || nTimeInt > submitTime + 180) {
            return shareError([20, "ntime out of range"]);
        }

        if(nonce.length !== 16) {
            return shareError([20, "incorrect size of nonce"]);
        }

        if(!job.registerSubmit(extraNonce1, extraNonce2, nTime, nonce)) {
            return shareError([22, "duplicate share"]);
        }

	var payload = extraNonce1 + extraNonce2;

	var blockRpcData = this.validJobs[jobId].rpcData;

	var blockHeader = Buffer.concat([
	    Buffer.from(blockRpcData.part1, "hex"),
	    Buffer.from(payload, "hex"),
	    Buffer.from(blockRpcData.part3, "hex"),
	    Buffer.from(nTime, "hex").swap32(),
	    Buffer.from(nonce, "hex").slice(4).swap32()
	]);


        randomhash.hashAsync(blockHeader, function (err, blockHash) {
            if(err) {
                return shareError([23, "low difficulty share"]);
            }

	    var block = false;

            var headerBigNum = bignum.fromBuffer(blockHash);

            var shareDiff = diff1 / headerBigNum.toNumber();

            //Check if share is a block candidate (matched network difficulty)
            if(job.target.ge(headerBigNum)) {
	        // Hooray!
	        block = {
		    height: blockRpcData.block,
		    payload: payload,
		    timestamp: parseInt(nTime, 16),
		    nonce: parseInt(nonce, 16)
	        };
            } else {
                //Check if share didn't reach the miner's difficulty)
                if(shareDiff / difficulty < 0.99) {

                    //Check if share matched a previous difficulty from before a vardiff retarget
                    if(previousDifficulty && shareDiff >= previousDifficulty) {
                        difficulty = previousDifficulty;
                    } else {
                        return shareError([23, "low difficulty share of " + shareDiff]);
                    }

                }
            }

            _this.emit("share", {
                job: jobId,
                ip: ipAddress,
                port: port,
                worker: worker.address_pid,
                workerName: worker.worker_id,
                workerPass: worker.password,
                height: job.rpcData.block,
                difficulty: difficulty,
                shareDiff: shareDiff.toFixed(9),
                blockDiff: job.difficulty,
                blockHash: blockHash.toString("hex"),
            }, block);

            callback({result: true, error: null, blockHash: blockHash});
            return {result: true, error: null, blockHash: blockHash};

        });
    };
};
JobManager.prototype.__proto__ = EventEmitter.prototype;
