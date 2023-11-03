const axios = require("axios");
const {getActionProof2, getBmProof, verify, compressProof} = require("./ibcFunctions")
const {getShipHeavyProof} = require("./shipFunctions");


//Websocket handlers
async function handleHeavyProof(blockNum, redis, ws) {

    console.log("开始计算block_num :" + blockNum)
    const start_block_num = blockNum - 1; //start at previous block of block that a user wants to prove
    let result;
    const req_block_to_prove = {
        firehoseOptions: {start_block_num, include_filter_expr: "", fork_steps: ["STEP_NEW", "STEP_UNDO"]},
        block_num: start_block_num,
        redis,
    }

    // const exists = await redis.exists(blockNum + "L")


    // if(exists === 1){
    //     ws.send(JSON.stringify({result:"none"}));
    //     return;
    // }else{
    //     await redis.setex(blockNum + "L", 200,1);
    // }


    let blockID;
    //get block_to_prove block ID
    let checkBlock = await checkValidBlockRange(start_block_num);
    if (!checkBlock.available) {    
        await sleep(2000);
        console.log(`${start_block_num}checkBlock error ,wait 2s!`)
    }
    const asyncFunction = getShipHeavyProof(req_block_to_prove);

    result = await asyncFunction();

    if (result) {

        blockID = (await axios.get(`${process.env.LIGHTPROOF_API}?blocks=${blockNum}`)).data[0].id;

        let bmproofPromises = [];
        let btp = blockNum;

        for (var bftproof of result.blockproof.bftproof) {
            bmproofPromises.push(getBmProof(btp, bftproof.block_num));
            btp = bftproof.block_num;
        }

        let bmproofpaths = await Promise.all(bmproofPromises);

        //add bmproofpath to each bftproof
        result.blockproof.bftproof.forEach((bftproof, i) => {
            bftproof.bmproofpath = bmproofpaths[i];

            blockID = bftproof.id;
            delete bftproof.block_num;
            delete bftproof.id;
        });

        const res = compressProof({proof: result});
        console.log(`${blockNum}计算完成,共 (${result.block.transactions.length - 1}) 条交易`);

        await ws.send(JSON.stringify(res.proof));
    }else{
        ws.send(JSON.stringify({result:"none"}));
        return;
    }

}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
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
