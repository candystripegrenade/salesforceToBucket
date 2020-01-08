// DEPS
const { Storage } = require('@google-cloud/storage');
const { auth } = require('google-auth-library');
const jsforce = require('jsforce');
const { convertArrayToCSV } = require('convert-array-to-csv');
const { writeFile } = require('fs').promises;
const { tmpdir } = require('os');

// ENV
const {
  PROJECT_NAME,
  BUCKET_NAME,
  SF_OBJECT,
  SF_USERNAME,
  SF_PASSWORD,
  SF_TOKEN
} = process.env;

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
  Parses the environment, then yields the storage client options
  @returns: { Object }
*/
function getStorageClientOpts() {
  const {
    client_email,
    private_key
  } = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS);

  return {
    projectId: PROJECT_NAME,
    credentials: {
      client_email,
      private_key,
    }
  };
}

/**
  Yields the bucket to store the csv in
  @returns: { Promise }
*/
async function getBucket() {
  const storageOpts = getStorageClientOpts();
  const client = new Storage(storageOpts);
  const bucket = await client.bucket(BUCKET_NAME);

  return bucket;
}

/**
  Yields the sf report according to environmental cols definition
  @returns: { Promise }
*/
async function getSFReport() {
  try {
    const authString = `${SF_PASSWORD}${SF_TOKEN}`;
    const conn = new jsforce.Connection();
    const client = await conn.login(SF_USERNAME, authString);
    const report = await conn.sobject(SF_OBJECT).find();

    return convertArrayToCSV(report);

  } catch(e) {
    throw new Error(`getFSReport: ${e}`);
  }
}

/**
  Abstraction of all the mini utilities of the operation
  @param: { Object } body
  @returns: { Promise }
*/
async function getAndUploadReport(body) {
  try {
    const filename = `${SF_OBJECT}.csv`;
    const writePath = `${tmpdir()}/${filename}`;
    const storageOpts = getBucketOpts(filename);
    const bucket = await getBucket();
    const csv = await getSFReport();
    const f = await writeFile(writePath, csv);
    const exists = await bucket.file(filename).exists();

    // kinda weird that the exists promise returns an array of size 1
    if (exists[0]) {
        const kill = await bucket.file(filename).delete();
    }

    const upload = await bucket.upload(writePath, storageOpts);

    return upload;

  } catch(e) {
    throw new Error(`getAndUploadReportError: ${e}`);
  }
}

/**
  Retrieves the report, then dumps it into the respective bucket
  @param: { Object } req
  @param: { Object } res
  @returns: { Response }
*/
exports.uploadSFToBucket = (req, res) => {
  getAndUploadReport(req.body)
  .then(() => res.status(200).send('Uploaded SF Report'))
  .catch(e => res.status(500).send(`Error: ${e}`));
};
