import {} from "dotenv/config";
import AWS from "aws-sdk";
import moment from 'moment';
import zlib from 'zlib';
import { parse } from 'csv-parse/sync';
import axios from 'axios';

const s3 = new AWS.S3({
  accessKeyId: process.env.ACCESS_KEY_ID,
  secretAccessKey: process.env.SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION,
  bucket: process.env.AWS_BUCKET
});

export async function handler(event) {
  try {
    const requestBody = JSON.parse(event.body);
    let { allRecords = false, day = false } = requestBody;

    let yesterday = (allRecords && allRecords === 'true') ? '' : (moment().subtract(1, 'days').format('YYYY/MM/DD') + '/');
    const bucketName = `singular-s3-exports-wallbit`
    let prefix = 'singular_userlevel_data/attributions/' + yesterday // '2024/11/02/'

    let isTruncated = true;
    let continuationToken;
    let csvFileKeys = [] // csvFiles.map(file => file.Key);

    if (day && day !== '') {
      prefix = 'singular_userlevel_data/attributions/' + day
    }

    while (isTruncated) {
      if (continuationToken) params.ContinuationToken = continuationToken;

      const data = await s3.listObjectsV2({ Bucket: bucketName, Prefix: prefix }).promise();
      const csvFiles = data.Contents.filter(file => file.Key.endsWith('.gz'));

      for (const file of csvFiles) 
        csvFileKeys.push(file.Key)

      isTruncated = data.IsTruncated;
      continuationToken = data.NextContinuationToken;
    }
    
    console.log('Archivos CSV encontrados:', csvFileKeys);

    let singularRecords = []

    for (const fileKey of csvFileKeys) {
        const fileData = await s3.getObject({ Bucket: bucketName, Key: fileKey }).promise();
        const decompressedData = zlib.gunzipSync(fileData.Body);
        const records = parse(decompressedData.toString(), {
            columns: true, // Cambia esto según la estructura del CSV
            skip_empty_lines: true
        });
        //console.log("Contenido del archivo:", records);
        singularRecords = [...singularRecords, ...records]
    }

    console.log('ready to add records: ', singularRecords.length)

    //CLEANING DEL UUID
    singularRecords = singularRecords.map(item => ({
      ...item,
      custom_user_ids: item.custom_user_ids.replace(/"/g, '') // FIX Elimina todas las comillas dobles
    }))

    //A VECES TRAIA UNA COMA, PERO HDP DALE
    singularRecords = singularRecords.flatMap(item => {
      if (item.custom_user_ids.includes(",")) {
        return item.custom_user_ids.split(",").map(custom_user_ids => ({
          ...item,
          custom_user_ids: custom_user_ids.trim()
        }));
      } else {
        return item;
      }
    });

    const size = 1
    let count = 1

    while (singularRecords.length > 0) {
      const batch = singularRecords.splice(0, size);

      const response = await axios.post(process.env.WALLBIT_URL, { data: batch },
        {
          auth: {
            username: process.env.BOT_USER,
            password: process.env.BOT_PASS,
          },
        })

      //const ids = batch.map(obj => obj.etl_record_processing_hour_utc);
      console.log(`Lote enviado exitosamente:`, (count++)*size, response.data);
    }

    return {
      statusCode: 200,
      body: JSON.stringify({csvFileKeys}),
    };
  } catch (error) {
    console.log("ERROOOOOR:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: "Error",
        error: error.message,
      }),
    };
  }
}
