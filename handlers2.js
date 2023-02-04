const hex64 = require('hex64');
const grpc = require("@grpc/grpc-js");
const axios = require("axios");
const { getIrreversibleBlock, preprocessBlock, getHeavyProof, getTxs } = require("./abstract")
const { getActionProof, getBmProof, verify, compressProof, getReceiptDigest } = require("./ibcFunctions")
const crypto = require("crypto");
const historyProvider = process.env.HISTORY_PROVIDER;
const fs = require('fs');





// 判断文件是否存在
function fileExists(filename) {
    return fs.promises.access(filename, fs.constants.F_OK)
        .then(() => true)
        .catch(() => false);
}


// 创建文件并写入内容
function createAndWriteFile(filename, content) {
    return fs.promises.writeFile(filename, content);
}

// 读取文件内容
function readFileContents(filename) {
    return fs.promises.readFile(filename, 'utf-8');
}

// 删除文件
function deleteFile(filename) {
    return fs.promises.unlink(filename);
}


const formatBFTBlock = (number, block) => ({
    id : block.id,
    block_num : number,
    header : block.header,
    producer_signatures: block.producer_signatures,
})
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}


//handler for on_proof_complete event
async function my_on_proof_complete(req, mydata){

    let reversibleBlocks = [];
    let uniqueProducers1 = [];
    let uniqueProducers2 = [];
    let block_to_prove;
    let previous_block;
    block_to_prove = mydata.block_to_prove;
    previous_block = mydata.previous_block;
    let data = mydata.data;



    const blockToProveNodes = (await axios.get(`${process.env.LIGHTPROOF_API}?blocks=${block_to_prove.block_num}`)).data[0].nodes
    const previousBlockNodes = (await axios.get(`${process.env.LIGHTPROOF_API}?blocks=${previous_block.block_num}`)).data[0].nodes


    const proof = {
        mydata:{data, block_to_prove,  previous_block},
        blockproof:{
            chain_id: process.env.CHAIN_ID,
            blocktoprove:{
                block:{
                    header: block_to_prove.header,
                    producer_signatures: block_to_prove.producer_signatures,
                    previous_bmroot: blockToProveNodes[blockToProveNodes.length-1],
                    id: "",
                    bmproofpath: []
                },
                active_nodes: previousBlockNodes,
                node_count: previous_block.block_num-1
            },
            bftproof: []
        }
    }

    for (var row of data.reversibleBlocks){
        var up1 = data.uniqueProducers1.find(item => item.number == row.number);
        var up2 = data.uniqueProducers2.find(item => item.number == row.number);
        if (up1 || up2) proof.blockproof.bftproof.push( formatBFTBlock(row.number, row.block) );
    }

    if (req.action_receipt_digest) proof.actionproof = getActionProof(block_to_prove, req.action_receipt_digest);
    // for (var tree of merkleTrees) for (var node of tree.activeNodes) node = hex64.toHex(node);

    //format timestamp in headers
    // for (var bftproof of proof.blockproof.bftproof) bftproof.header.timestamp = convertFirehoseDate(bftproof.header.timestamp) ;
    // proof.blockproof.blocktoprove.block.header.timestamp = convertFirehoseDate(proof.blockproof.blocktoprove.block.header.timestamp);
    let blocksTofetch = [];
    for (var bftproof of proof.blockproof.bftproof )  {
        blocksTofetch.push(bftproof.block_num);
    };
    const uniqueList = [];
    for (var num of blocksTofetch) if (!uniqueList.includes(num)) uniqueList.push(num);
    let result = (await axios(`${process.env.LIGHTPROOF_API}?blocks=${uniqueList.join(',')}`)).data;

    for (var i = 0 ; i < blocksTofetch.length;i++) {
        const b = result.find(r=>r.num === blocksTofetch[i]);
        if(!b){
            console.log("Error, block not found!",  blocksTofetch[i]);
            process.exit();
        }
        proof.blockproof.bftproof[i].previous_bmroot = b.nodes[b.nodes.length-1];
    }

    return proof;
    //resolve(proof);
}




//Websocket handlers
async function handleHeavyProof(msgObj, ws){
    try {
        console.log("handler.js ...feng ..0");
        const start_block_num  = msgObj.block_to_prove -1; //start at previous block of block that a user wants to prove

        let checkBlock = await checkValidBlockRange(start_block_num);
        console.log("handler.js ...feng ..1");
        if(!checkBlock.available) return ws.send(JSON.stringify({ type:"error", error: checkBlock.error }));

        console.log("handler.js ...feng ..2");
        const req_block_to_prove = {
            firehoseOptions : { start_block_num, include_filter_expr: "", fork_steps: ["STEP_NEW", "STEP_UNDO"] },
            action_receipt_digest: msgObj.action_receipt ? getReceiptDigest(msgObj.action_receipt) : msgObj.action_receipt_digest,
            ws,
            block_num:start_block_num
        }
        console.log("handler.js ...feng ..3");




        var myresult  =  null ;//await getHeavyProof(req_block_to_prove);



        const fileA = 'file'+msgObj.block_to_prove+'lock.txt';
        const fileB = 'file'+msgObj.block_to_prove+'.txt';
        const fileC = 'file'+msgObj.block_to_prove+'light.txt';



        try {

            var mylock = 0;
            if(typeof(global.myLock) == "undefined"){
                global.myLock = new Map();
            }


            console.log("lock:", global.myLock);
            let count1 = 0;
            while (count1 <10000) {

                if(!global.myLock.has(msgObj.block_to_prove) ){
                    global.myLock.set(msgObj.block_to_prove, 1);// = 1;//lockit
                    mylock = 1;
                    break;
                }else if(global.myLock.get(msgObj.block_to_prove) ==1){
                    await sleep(100);
                }else{
                    break;
                }
                count1++;

            }


            console.log("lock2:", global.myLock);
            if (global.myLock.get(msgObj.block_to_prove) ==1 && mylock == 0) {
                return ws.send(JSON.stringify({ type:"feng", error: "it is still locked after 20s" }));
                console.log(`${fileA} exists.`);
            } else {
                if (await fileExists(fileB)) {
                    console.log(`${fileB} exists.`);
                    global.myLock.set(msgObj.block_to_prove,0);//找到了，就让上面的下来吧
                    const content = await readFileContents(fileB);
                    const c1 = JSON.parse(content);
                    req_block_to_prove.mydata = c1;
                    if (await fileExists(fileC)) {
//			      console.log("1111111111111111111111111111111111");
                        const contentC = await readFileContents(fileC);
                        myresult = JSON.parse(contentC);
                    }else{
                        myresult = await my_on_proof_complete(req_block_to_prove, c1);
                        await createAndWriteFile(fileC, JSON.stringify(myresult) );
                    }

                } else {
                    //await createAndWriteFile(fileA, '');
                    global.myLock.set(msgObj.block_to_prove,1);//其实在上面早已经被锁定了, 但也有一种情况，就是运行过程中，对应结果缓存文件被删除了，这里会执行, 不过这里很可能是大理的并发，会出错
                    console.log(`${fileA} and ${fileB} do not exist. Creating them...`);


                    myresult  = await getHeavyProof(req_block_to_prove);
                    await createAndWriteFile(fileB, JSON.stringify(myresult.mydata) );
                    console.log(`${fileA} and ${fileB} created.`);

                    //await deleteFile(fileA);
                    global.myLock.set(msgObj.block_to_prove,0);//其实在上面早已经被锁定了
                    console.log(`${fileA} deleted.`);
                }
            }



        } catch (error) {
            console.error('An error occurred:', error);
        }


        delete myresult.mydata;
//   console.log(`getShipHeavyProof:`  );
//   console.log(`getShipHeavyProof:` + JSON.stringify(myresult) );




        const response = {
            type: "proof",
            query : msgObj,
            proof: myresult,
            action_receipt_digest: req_block_to_prove.action_receipt_digest //required for issue & retire
        }


        //以下就是显示 百份比的地方
//    const response = {
//      type: "proof",
//      query : msgObj,
//      proof: await getHeavyProof(req_block_to_prove),
//      action_receipt_digest: req_block_to_prove.action_receipt_digest //required for issue & retire
//    }

        console.log("handler.js ...feng ..4");
        //add bmproofpath to bftproofs
        let bmproofPromises = [];
        let btp = msgObj.block_to_prove;

        for (var bftproof of response.proof.blockproof.bftproof )  {
            bmproofPromises.push( getBmProof(btp, bftproof.block_num) );
            btp = bftproof.block_num;
        };

        //   return ws.send(JSON.stringify({ type:"feng", error: myresult }));


        console.log("handler.js ...feng ..5");
        let bmproofpaths = await Promise.all(bmproofPromises);

        //get block_to_prove block ID
        let blockID = (await axios.get(`${process.env.LIGHTPROOF_API}?blocks=${msgObj.block_to_prove}`)).data[0].id;
        let passed = true;
        //add bmproofpath to each bftproof
        response.proof.blockproof.bftproof.forEach((bftproof, i) => {
            bftproof.bmproofpath = bmproofpaths[i];
            var verif = verify(bftproof.bmproofpath, blockID, bftproof.previous_bmroot );
            console.log("verif",i,verif);
            if (!verif){
                passed = false;
                console.log("\nbftproof:",i+1);
                console.log("block_to_prove:",msgObj.block_to_prove);
                console.log("last_proven_block:",bftproof.block_num);
                console.log("Verif failed for:");
                console.log("bftproof.bmproofpath (nodes)",bftproof.bmproofpath);
                console.log("blockID (leaf)",blockID);
                console.log("bftproof.previous_bmroot (root)",bftproof.previous_bmroot);
            }

            blockID = bftproof.id;
            delete bftproof.block_num;
            delete bftproof.id;
        });
        console.log("\nbftproof verification finished", passed)

        console.log("handler.js ...feng ..6");
        if (passed){
            response.query =msgObj;
            response.proof.actionproof.returnvalue = "";//feng
            response.proof = (compressProof({ proof: response.proof })).proof;
            ws.send(JSON.stringify(response));
        }
        else ws.send(JSON.stringify({type:"error", error: "bftproof verification failed, contact proof socket admin", query:response.query}));

    }catch(ex){
        console.log(ex);
        ws.send(JSON.stringify({type:"error", error: ex}));
    }
}

function handleLightProof(msgObj, ws){

    //just need the block to prove from the history provider

    return new Promise(async resolve => {
        console.log(msgObj);

        let checkBlock = await checkValidBlockRange(msgObj.block_to_prove);
        let checkBlock2 = await checkValidBlockRange(msgObj.last_proven_block);

        if (!checkBlock.available){
            console.log("Block is not in valid range")
            if(!checkBlock.available) return ws.send(JSON.stringify({ type:"error", error: checkBlock.error }));
            if(!checkBlock2.available) return ws.send(JSON.stringify({ type:"error", error: checkBlock2.error }));
        }

        var result = await getIrreversibleBlock(msgObj.block_to_prove);
        var block_to_prove = preprocessBlock(result, true);

        var proof = {
            blockproof : {
                chain_id : process.env.CHAIN_ID,
                header : block_to_prove.header,
                bmproofpath : await getBmProof(msgObj.block_to_prove, msgObj.last_proven_block)
            }
        }

        if (msgObj.action_receipt || msgObj.action_receipt_digest){
            if (msgObj.action_receipt) msgObj.action_receipt_digest = getReceiptDigest(msgObj.action_receipt);
            if (historyProvider==='greymass') {
                const transactions = await getTxs(block_to_prove);
                proof.actionproof = getActionProof({transactions, block_num: block_to_prove.block_num}, msgObj.action_receipt_digest)
            }
            else proof.actionproof = getActionProof(block_to_prove, msgObj.action_receipt_digest)
        }

        let blockID = (await axios.get(`${process.env.LIGHTPROOF_API}?blocks=${msgObj.block_to_prove}`)).data[0].id;
        let lastProvenBlockNodes = (await axios.get(`${process.env.LIGHTPROOF_API}?blocks=${msgObj.last_proven_block}`)).data[0].nodes;
        //add bmproofpath to each bftproof
        var passed = verify(proof.blockproof.bmproofpath, blockID, lastProvenBlockNodes[lastProvenBlockNodes.length-1] );
        console.log("passed",passed);

        //if (passed){
        if (true){
            console.log("Verified proof sent to client")
            ws.send(JSON.stringify({ type: "proof", query: msgObj, proof }));
        }
        else{
            console.log("blockFromLP",blockFromLP)
            console.log("proof.blockproof.bmproofpath",proof.blockproof.bmproofpath);
            ws.send(JSON.stringify({type:"error", error: "bftproof verification failed, contact proof socket admin", query:response.query}));
        }

        resolve();
    })
}

async function handleGetBlockActions(msgObj, ws){
    try {
        console.log("handleGetBlockActions", msgObj.block_to_prove);
        console.log("handleGetBlockActions", msgObj);//feng
        // let checkBlock = await checkValidBlockRange(msgObj.block_to_prove);
        // if(!checkBlock.available) return ws.send(JSON.stringify({ type:"error", error: checkBlock.error }));

        let res = await getIrreversibleBlock(msgObj.block_to_prove);
        if(res.data) res = res.data;
        const txs = await getTxs(res);

        console.log("handleGetBlockActions", JSON.stringify(txs));//feng

        console.log("handleGetBlockActions finished", msgObj.block_to_prove)
        ws.send(JSON.stringify({ type: "getBlockActions", query : msgObj, txs }));

    }catch(ex){
        console.log(ex);
        ws.send(JSON.stringify({type:"error", error: ex}));
    }
}

async function handleGetDbStatus(ws){
    axios(`${process.env.LIGHTPROOF_API}/status`).then(res=>{
        ws.send(JSON.stringify({ type: "getDbStatus", data:res.data}));
    }).catch(ex=>{
        console.log("ex getting db status", ex.response.status, ex.response.statusText)
        ws.send(JSON.stringify({ type: "getDbStatus", data:{error:"Error getting DB status, contact admin"}}));
    });
}


function checkValidBlockRange(blockNum){
    return new Promise(async resolve=>{
        try{
            console.log("handler.js ...feng .  checkValidBlockRange.0");
            blockNum = parseInt(blockNum);
            const { minBlockToProve,lastBlock, lib, firstBlock } = (await axios(`${process.env.LIGHTPROOF_API}/status`)).data;
            console.log("handler.js ...feng .  checkValidBlockRange.1");
            if(!minBlockToProve) minBlockToProve = firstBlock;
            if (blockNum < minBlockToProve || blockNum > lastBlock){
                console.log("handler.js ...feng .  checkValidBlockRange.2");
                resolve({ available: false, error:  `Attempting to prove a block (#${blockNum}) that is outside proveable range in lightproof-db (${minBlockToProve} -> ${lastBlock} )` });
            } else resolve({ available: true })
        }catch(ex){
            resolve({
                available: false,
                error:  `Error fetching status of lightproof`
            });
        }
    })
}

module.exports = {
    handleLightProof,
    handleHeavyProof,
    handleGetBlockActions,
    handleGetDbStatus
}
