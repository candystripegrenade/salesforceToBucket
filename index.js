// DEPS
const https = require('https');
// For Dev (locally)
//const http = require('http');
const { Storage } = require('@google-cloud/storage');
const { unlink } = require('fs').promises;
const { createWriteStream, createReadStream } = require('fs');
const url = require('url');
const { tmpdir } = require('os');
const { join } = require('path');

// ENV
const {
  PROJECT_NAME,
  SF_SEARCH_URL,
  GOOGLE_APPLICATION_CREDENTIALS
} = process.env;

// GLOBALS
let credentials, storageOpts, storageClient, httpsAgent;

/**
  Yields credentials needed to auth
  @returns: { Object }
*/
function getCredentials() {
  if (credentials !== undefined) {
    return credentials;
  }
  credentials = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS);
  return credentials;
}

/**
  Parses the environment & yields storage client options
  @returns: { Object }
*/
function getStorageClientOpts() {
  if (storageOpts !== undefined) {
    return storageOpts;
  }
  storageOpts = {
    projectId: PROJECT_NAME,
    credentials: getCredentials()
  };
  return storageOpts;
}

/**
  Yields the storageClient (if exists)
  @returns: { Object }
*/
function getStorageClient() {
  if (storageClient !== undefined) {
    return storageClient;
  }
  storageClient = new Storage(getStorageClientOpts());
  return storageClient;
}

/**
  Yields the keepAlive connection for subsequent requests
  @returns: { Object }
*/
function getHttpsAgent() {
  if (httpsAgent !== undefined) {
    return httpsAgent;
  }
  //httpsAgent = new http.Agent({ keepAlive: true });
  httpsAgent = new https.Agent({ keepAlive: true });
  return httpsAgent;
}

/**
  Retrieves the bucket options for storage ops
  @param: { String } filename
  @returns: { Object }
*/
function getBucketOpts(filename) {
  return {
    destination: filename,
    resumable: false,
    private: true,
    predefinedAcl: 'projectPrivate'
  };
}

/**
  Yields bucket with default params
  @param: { String } bucketName
  @returns: { Promise<Object> }
*/
async function getBucket(bucketName) {
  try {
    const client = getStorageClient();
    const bucket = await client.bucket(bucketName);
    return bucket;
  } catch(err) {
    throw new Error(`from getBucket => ${ err }`);
  }
}

/**
  yields the query for everything we care about
  @returns: { String }
*/
function getAllOpportunitiesQuery() {
  return `
    SELECT
      X18_Digit_Opportunity_ID__c,
      OwnerId,
      AccountId,
      Bill_To_Account_Name__c,
      Name,
      ForecastCategoryName,
      Owner_Territory__c,
      Type,
      LeadSource,
      StageName,
      Amount,
      Holdover__c,
      Age__c,
      Created_Date__c,
      Closed_Won_Stage_Date__c,
      SalesLoft1__Most_Recent_Cadence_Name__c,
      Custom_Forecast_Category__c,
      Product_Sales_Play_s__c,
      SalesLoft1__Most_Recent_Last_Completed_Step__c
    FROM Opportunity
  `;
}

/**
  Retrieve opportunity history from <url>
  @returns: { Promise<Stream> }
*/
async function getAllOpportunitiesRequest() {
  try {
    const customQuery = getAllOpportunitiesQuery();
    const params = { isBulk: true, customQuery };
    const payload = { fn: 'getOpportunityHistory', params };
    const { hostname, port, path } = url.parse(SF_SEARCH_URL);
    const agent = getHttpsAgent();
    const headers = { 'Content-Type': 'application/json' };
    const method = 'POST';
    const httpOpts = { method, path, hostname, port, headers, agent };
    return await new Promise((resolve, reject) => {
      //const req = http.request(httpOpts, resolve);
      const req = https.request(httpOpts, resolve);
      req.on('error', reject);
      req.write(JSON.stringify(payload));
      req.end();
    });
  } catch(err) {
    throw new Error(`from getAllOpportunitiesRequest => ${ err }`);
  }
}

/**
  makes request to get the sf data and upload it as a csv file to gcp bucket
  @param: { Object } params
  @returns: { Object }
*/
async function oppHistory(params) {
  try {
    const { bucketName } = params;
    const filename = 'opportunityhistory.csv';
    const writePath = join(tmpdir(), filename);
    const sClient = getStorageClient();
    const bOpts = getBucketOpts(filename);
    const bucket = await getBucket(bucketName);
    const req = await getAllOpportunitiesRequest();
    const exists = await bucket.file(filename).exists();
    const file = req.pipe(createWriteStream(writePath));
    if (exists[0]) {
      await bucket.file(filename).delete();
    }
    await bucket.upload(writePath, bOpts);
    await unlink(writePath);
    return createReadStream(Buffer.from(`finished writing file ${ filename }`, 'utf8'));
  } catch(err) {
    throw new Error(`from oppHistory => ${ err }`);
  }
}

/**
  The functions available through Entrypoint
  @returns: { Object }
*/
function getFns() {
  return {
    oppHistory
  };
}

/**
  Retrieves the report, then dumps it into the respective bucket
  @param: { Object } req
  @param: { Object } res
  @returns: { Stream }
*/
async function sfToBucket(req, res) {
  try {
    const fns = getFns();
    const { fn, params } = req.body;
    const reader = await fns[fn](params);
    const resText = 'text/plain';
    res.setHeader( 'Content-Type', resText);
    reader.pipe(res);
  } catch(err) {
    console.info(`${ err }`);
    res.setHeader('Content-Type', 'text/plain');
    res.status(500).send(`${ err }`);
  }
}

// EXPORTS
exports.sfToBucket = sfToBucket;
