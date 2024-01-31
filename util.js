const fs = require('fs');
const decompress = require("decompress");
const axios = require('axios');

const download = async (token, url, fileName) => {
  try {
    const response = await axios({
      url,
      method: 'GET',
      responseType: 'stream',
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });

    if (response.status !== 200) {
      throw new Error(`Error downloading file: ${response.status}`);
    }

    const writer = fs.createWriteStream(`./files/${fileName}`);
    response.data.pipe(writer);

    return new Promise((resolve, reject) => {
      console.log(`Downloading file: ${fileName}`);
      writer.on('finish', resolve);
      writer.on('error', reject);
    });
  } catch (error) {
    console.error(`Error downloading file: ${error}`);
    throw error;
  }
};

const extractZip = async (fileName) => {
  return new Promise((resolve, reject) => {
    decompress(`./files/${fileName}`, './files/unzipped')
      .then(() => {
        console.log(`Zip file extracted: ${fileName}`);
        resolve();
      })
      .catch((error) => {
        console.error(`Error extracting zip file: ${error}`);
        reject(error);
      });
  });
};

const objectsToCsv = (data) => {
  const csvRows = [];

  // get the headers
  const headers = Object.keys(data[0]);
  csvRows.push(headers.join(','));
  // loop over the rows
  for (const row of data) {
    const values = headers.map(header => {
      return ('' + row[header]).replace(/"/g, '');
    });
    csvRows.push(values.join(','));
  }
  return csvRows.join('\n');
};

const readCsv = (fileName) => {
  const csv = fs.readFileSync(`./files/unzipped/${fileName}`, 'utf-8');
  const lines = csv.split('\n');
  const headers = lines[0].split(',');
  const data = [];
  for (let i = 1; i < lines.length; i++) {
    const values = lines[i].split(',');
    if (values.length === headers.length) {
      const obj = {};
      for (let j = 0; j < headers.length; j++) {
        const header = headers[j];
        const value = values[j];

        obj[header] = value;
      }

      data.push(obj);
    }

  }
  return data;
};

const sleep = (milliseconds) => {
  return new Promise(resolve => setTimeout(resolve, milliseconds));
};

module.exports = {
  objectsToCsv,
  extractZip,
  decompress,
  download,
  readCsv,
  sleep,
};