const axios = require("axios");
const {getActionProof2, getBmProof, verify, compressProof, getReceiptDigest} = require("./ibcFunctions")
const {getShipHeavyProof} = require("./shipFunctions1");
const {Worker, isMainThread, parentPort, workerData} = require('worker_threads');

const formatBFTBlock = (number, block) => ({
    id: block.id,
    block_num: number,
    header: block.header,
    producer_signatures: block.producer_signatures,
})

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

let count = 0;

//Websocket handlers
async function handleHeavyProof(msgObj, redis, ws) {
    const start_block_num = msgObj.block_to_prove - 1; //start at previous block of block that a user wants to prove
    let res = await redis.setnx(msgObj.block_to_prove + "L", 1);
    let result;
    const req_block_to_prove = {
        firehoseOptions: {start_block_num, include_filter_expr: "", fork_steps: ["STEP_NEW", "STEP_UNDO"]},
        action_receipt_digest: msgObj.action_receipt_digest ? msgObj.action_receipt_digest : getReceiptDigest(msgObj.action_receipt),
        block_num: start_block_num,
        redis,
    }

    const response = {
        type: "proof",
        query: msgObj,
        action_receipt_digest: req_block_to_prove.action_receipt_digest //required for issue & retire
    }
    let blockID;
    //get block_to_prove block ID
    let passed = true;

    //加锁成功
    if (res == 1) {

        console.log("lock success... msgObj.block_to_prove = " + msgObj.block_to_prove)
        await redis.expire(msgObj.block_to_prove + "L", 10000);

        const beginTime = +new Date();
        let checkBlock = await checkValidBlockRange(start_block_num);
        if (!checkBlock.available) {
            return "check error";
        }
        const asyncFunction = getShipHeavyProof(req_block_to_prove);
        result = await asyncFunction();

        const endTime = +new Date();

        console.log(msgObj.block_to_prove + "用时 " + (endTime - beginTime) / 1000 + "s")

        // console.log(JSON.stringify(result))

        const blockToProveNodes = (await axios.get(`${process.env.LIGHTPROOF_API}?blocks=${result.mydata.block_to_prove.block_num}`)).data[0].nodes;
        const previousBlockNodes = (await axios.get(`${process.env.LIGHTPROOF_API}?blocks=${result.mydata.previous_block.block_num}`)).data[0].nodes;

        result.blockToProveNodes = blockToProveNodes;
        result.previousBlockNodes = previousBlockNodes;
        response.proof = result;
        //get block_to_prove block ID
        blockID = (await axios.get(`${process.env.LIGHTPROOF_API}?blocks=${msgObj.block_to_prove}`)).data[0].id;

        let bmproofPromises = [];
        let btp = msgObj.block_to_prove;

        for (var bftproof of result.blockproof.bftproof) {
            bmproofPromises.push(getBmProof(btp, bftproof.block_num));
            btp = bftproof.block_num;
        }

        let bmproofpaths = await Promise.all(bmproofPromises);

        //add bmproofpath to each bftproof
        result.blockproof.bftproof.forEach((bftproof, i) => {
            bftproof.bmproofpath = bmproofpaths[i];
            var verif = verify(bftproof.bmproofpath, blockID, bftproof.previous_bmroot);
            if (!verif) {
                passed = false;
                console.log("\nbftproof:", i + 1);
                console.log("block_to_prove:", msgObj.block_to_prove);
                console.log("last_proven_block:", bftproof.block_num);
                console.log("Verif failed for:");
                console.log("bftproof.bmproofpath (nodes)", bftproof.bmproofpath);
                console.log("blockID (leaf)", blockID);
                console.log("bftproof.previous_bmroot (root)", bftproof.previous_bmroot);
            }

            blockID = bftproof.id;
            delete bftproof.block_num;
            delete bftproof.id;
        });
        response.tree = global[msgObj.block_to_prove + "tree"];
        response.transactions = global[msgObj.block_to_prove + "transactions"];
        await pass(passed, response, ws);
        /*res.block_to_prove = result.mydata.block_to_prove;
        res.query = result.query;

        result = res;

        await redis.setex(msgObj.block_to_prove, 200, JSON.stringify(result));
        await redis.del(msgObj.block_to_prove + "L")
        delete req_block_to_prove.redis;*/
        // const worker = new Worker('./worker.js', );

    } else {
        return;
        /*setTimeout(
            async () => {
                await waitForLock()
            },
            15000
        )*/
    }


    async function waitForLock() {
        const resultLock = await redis.exists(msgObj.block_to_prove + "L");
        if (resultLock === 0) {
            if (global[msgObj.block_to_prove] == undefined) {
                global[msgObj.block_to_prove] = JSON.parse(await redis.get(msgObj.block_to_prove));
            }
            const result = await my_on_proof_complete1(req_block_to_prove, global[msgObj.block_to_prove]);
            result.actionproof.returnvalue = ""
            delete result.mydata;
            response.proof = result;
            await toSequence(response, ws);
        } else {
            setTimeout(waitForLock, 200); // 非阻塞等待一秒钟
        }
    }

}


async function pass(passed, response, ws) {
    if (passed) {
        response.proof.actionproof.returnvalue = "";//feng
        const res = compressProof({proof: response.proof});
        response.proof = res.proof;

        await toSequence(response, ws);

        return res.proof;
    } else {
        return "error"
    }
}

async function toSequence(res, ws) {
    // await redis.rpush("actionProofs", JSON.stringify(res));
    // console.log()
    // console.log(res)
    // console.log()

    count++;
    console.log("已处理:" + count)
    await ws.send(JSON.stringify(res));
}

//handler for on_proof_complete eventmy_on_proof_complete
async function my_on_proof_complete(req, mydata) {
    let block_to_prove = mydata.mydata.block_to_prove;
    let previous_block = mydata.mydata.previous_block;
    let data = mydata.mydata.data;

    let blockToProveNodes = mydata.blockToProveNodes;
    let previousBlockNodes = mydata.previousBlockNodes;

    const proof = {
        mydata: {data, block_to_prove, previous_block},
        blockproof: {
            chain_id: process.env.CHAIN_ID,
            blocktoprove: {
                block: {
                    header: block_to_prove.header,
                    producer_signatures: block_to_prove.producer_signatures,
                    previous_bmroot: blockToProveNodes[blockToProveNodes.length - 1],
                    id: "",
                    bmproofpath: []
                },
                active_nodes: previousBlockNodes,
                node_count: previous_block.block_num - 1
            },
            bftproof: []
        }
    }

    for (var row of data.reversibleBlocks) {
        var up1 = data.uniqueProducers1.find(item => item.number == row.number);
        var up2 = data.uniqueProducers2.find(item => item.number == row.number);
        if (up1 || up2) proof.blockproof.bftproof.push(formatBFTBlock(row.number, row.block));
    }

    if (req.action_receipt_digest) proof.actionproof = getActionProof2(block_to_prove, req.action_receipt_digest);

    let blocksTofetch = [];
    for (var bftproof of proof.blockproof.bftproof) {
        blocksTofetch.push(bftproof.block_num);
    }
    const uniqueList = [];
    for (let num of blocksTofetch) if (!uniqueList.includes(num)) uniqueList.push(num);

    proof.blockproof.bftproof = mydata.blockproof.bftproof;

    return proof;
}

async function my_on_proof_complete1(req, mydata) {

    let block_to_prove = mydata.block_to_prove;
    const proof = mydata;
    delete proof.mydata;
    if (req.action_receipt_digest) proof.actionproof = getActionProof2(block_to_prove, req.action_receipt_digest);
    return proof;
}

function checkValidBlockRange(blockNum) {
    return new Promise(async resolve => {
        try {
            blockNum = parseInt(blockNum);
            const {
                minBlockToProve,
                lastBlock,
                lib,
                firstBlock
            } = (await axios(`${process.env.LIGHTPROOF_API}/status`)).data;
            if (!minBlockToProve) minBlockToProve = firstBlock;
            if (blockNum < minBlockToProve || blockNum > lastBlock) {
                resolve({
                    available: false,
                    error: `Attempting to prove a block (#${blockNum}) that is outside proveable range in lightproof-db (${minBlockToProve} -> ${lastBlock} )`
                });
            } else resolve({available: true})
        } catch (ex) {
            resolve({
                available: false,
                error: `Error fetching status of lightproof`
            });
        }
    })
}

module.exports = {
    handleHeavyProof
}
