/***
 *    KOK - 2019-03-12
 */

'use strict'

/**
 Node Modules
 **/

const path = require('path');

/**
 Local Modules
 **/

//*************************************************
const config = require('config/config');
//*************************************************
const logMgr = config.getConstructed('logger');
const logger = config.logMsg;
const sleep = config.sleep;
//-------------------------------------------------

//*************************************************
const mongoMgr = require('applib/mongodbCnxMgr');
//*************************************************
const getMongoDbCnx = mongoMgr.getDbCnx;
const getDbName = mongoMgr.getDbName;
//-------------------------------------------------

//************************************************
const filesMgr = require('applib/filesManager');
//************************************************
const getFirstAvailableFile = filesMgr.getFirstAvailableFile;
const moveFile = filesMgr.moveFile;
const cleanFileProcessQ = filesMgr.cleanFileProcessQ;
//-------------------------------------------------

//*************************************************
const commonsLib = require('applib/commons');
//*************************************************
const onFileProcErr = commonsLib.onFileProcErr;
//-------------------------------------------------

//**********************************************************************
const mongoCommonCollections = require('applib/mongoCommonCollections');
//**********************************************************************
const createJob = mongoCommonCollections.createJob;
const updateJob = mongoCommonCollections.updateJob;
const getSequence = mongoCommonCollections.getSequence;
//----------------------------------------------------------------------


//*************************************************
const fileParser = require('applib/cdrDecoder/fileParser');
//*************************************************
const importFeedFile = fileParser.importFeedFile;
//-------------------------------------------------


/**
 * Local declarations
 *
 */
let localDbCnx;
let procFilesH;

module.exports = {
  init: init,
  processNextFile: processNextFile
};


/**
 * Local Functions
**/
async function init() {
  try {
    localDbCnx = await getMongoDbCnx();
    logger.info(`(Connected OK to mongo database ==> ${(getDbName(localDbCnx))}`);
    procFilesH = localDbCnx.collection('processedFilesH');
    await filesMgr.init();
    await fileParser.init();
  }
  catch(err){
    logger.unexpectedErr(err);
    throw err;
  }

}

async function processNextFile(filesPath, reverseTimeOrder){
  let localErr;
  let fileToProc;
  let processResultsDoc;

  try {

    try {
      fileToProc = await getFirstAvailableFile(filesPath,reverseTimeOrder);

      if (!fileToProc) return;

      const startProcFileTime = new Date();
      logger.info(`###### File Process Start : ========> : ${startProcFileTime}`);
      logger.info(`Processing file : ${fileToProc.fileName}`);

      //Creates JOB
      const procName = 'input_processor';
      // const start = startProcFileTime;
      let jobPayload = {
        processID: myProcName,
        file: fileToProc.fileName,
        starDate: startProcFileTime,
        [procName]: {
          startTime: startProcFileTime,
          status: 'processing'
        }
      };
      const jobId = await createJob(jobPayload);
      logMgr.taskId = jobId || null;//KOK 2019-07-17

      //--------------------------------------------

      //Process file
      const summary = await importFeedFile(fileToProc, jobId);

      //Moves file to destination directory depending on error status
      let ret = summary.error ? summary.error.flag : 'OK';
      const toPath = ret === onFileProcErr.FAIL ? filesPath.errPath : filesPath.okPath;
      // const mvRet = moveFile(fileToProc.fileName, filesPath.repoPath, toPath);
      // const msg = mvRet ? 'File moved Successfully' : 'Error moving file';
      moveFile(fileToProc.fileName, filesPath.repoPath, toPath);
      logger.info('File moved Successfully');

      if(summary.totals.siteName){
        jobPayload[procName].siteName = summary.totals.siteName;
        delete summary.totals.siteName;
      }

      processResultsDoc = createProcessDoc(startProcFileTime, summary);
      //Writes file's processing results into JOB's collection
      logger.info(`Process finished for file : ${fileToProc.fileName} -- Summary : ${(JSON.stringify(summary))}`);

      jobPayload[procName].status = ret === onFileProcErr.FAIL  ? 'error': 'finished';
      const summSrc = (jobPayload[procName].summary = Object.assign({}, processResultsDoc));
      // jobPayload[procName].summary = clonedForJob;

      //Moves resulst from summary to to Job's process
      const mvOutArr = [
        'startTime',
        'endTime',
        'elapsedTimeInMillisecs',
        'elapsedTimeInSecs',
        'validTransacPerc',
        'tps'
      ];
      mvOutArr.forEach(key =>{
        if(summSrc[key]){
          jobPayload[procName][key] = summSrc[key];
          delete summSrc[key];
        }
      });

      await updateJob(jobId, jobPayload);

      // if(Object.keys(processResultsDoc).length > 0)  updateHistory(processResultsDoc);

      logMgr.taskId = null;//KOK - 2019-07-17

      return {fileName: fileToProc.fileName, histDoc: processResultsDoc};

      //updateProcStatus({fileName: fileName, histDoc: histDoc}))
    }
    catch (err) {
      logger.unexpectedErr(err);
      localErr = err;
    }
    finally {
      if(fileToProc) {
        await cleanFileProcessQ(fileToProc.fileName, localErr);
        logger.info('Pending Files Q - FINAL update :  SUCCESSFUL');
      }
    }
    if(!fileToProc) return null;
    return {fileName: fileToProc.fileName, histDoc: processResultsDoc};
  }
  catch (err) {
    logger.unexpectedErr(err);
    throw err;
  }

}

function createProcessDoc(startProcFileTime, summary) {

  try {
    const doc = {};
    const tot = summary.totals;
    doc.startTime = startProcFileTime;
    doc.endTime = new Date();
    doc.elapsedTimeInMillisecs = (doc.endTime - doc.startTime);
    doc.elapsedTimeInSecs = doc.elapsedTimeInMillisecs / 1000;
    doc.validTransacPerc = (tot.recordsOK / (tot.rawLines || 1)) * 100;

    doc.tps = (tot.transactions || -1) / (doc.elapsedTimeInSecs || 1);

    Object.keys(summary).forEach(key => {
      if (typeof summary[key] === 'object') {
        doc[key] = summary[key];
      }
    });

    return doc;
  }
  catch(err){
    logger.unexpectedErr(err);
    throw(err);
  }
}

async function updateHistory(doc) {

  try{
    await procFilesH.insertOne(doc)//, (err, res) => {
  }
  catch(err) {
    logger.unexpectedErr(err);
    throw err;
  }
}
