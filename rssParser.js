let Parser = require('rss-parser');
let moment = require('moment');
const {BigQuery} = require('@google-cloud/bigquery');
let parser = new Parser();
const urls = [
    {
        program: "Financially Inclined",
        feed: "https://www.marketplace.org/feed/podcast/financially-inclined"
    }
];
const projectId = `apmg-data-warehouse`;
const bigquery = new BigQuery({
    projectId: projectId
});
const datasetId = 'apm_podcasts';
const tableId = 'episode_titles';
async function insertRowsAsStream(param) {
    let rows = param;
    await bigquery
        .dataset(datasetId)
        .table(tableId)
        .insert(rows);
    console.log(`Inserted ${rows.length} rows`);
    return 'Ok';
}
let removeDups = async () => {
    let sqlQuery = `CREATE OR REPLACE TABLE ${projectId}.${datasetId}.${tableId} AS SELECT Episode, uri_path, Program, Title FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY Program, Episode, uri_path) row_number FROM ${projectId}.${datasetId}.${tableId} ) WHERE row_number = 1`;
    const options = {
        query: sqlQuery,
        location: 'US'
    };
    const [rows] = await bigquery.query(options);
    console.log(`Table is now ${rows.length} rows`);
}
let parseRSS = (url) => {
    return new Promise((resolve, reject) => {
        let dataToAdd = [];
        let parseUri = new RegExp('\/o(,?.*)');
        function createRecord(url, item) {
            return {
                program: url.program,
                title: item.title,
                uri_path: parseUri.exec(item.enclosure.url)[1],
                episode: moment(item.pubDate).format('YYYY-MM-DD')
            }
        }
        parser.parseURL(url.feed, (err, feed) => {
            if (err) {
                reject(err);
            } else if (feed.title = url.program) {
                feed.items.forEach(item => {
                    if (item.hasOwnProperty('enclosure')) {
                        var obj = createRecord(url, item);
                        dataToAdd.push(obj)
                    }
                    resolve(dataToAdd);
                });
            }
        });
    })
}
   let dataArray = [];
   urls.forEach(async (url) => {
       let feed = parseRSS(url)
       dataArray.push(feed);
   })
    module.exports = (() => {
        Promise.all(dataArray).then((data) => {
            console.log('got the data', data);
            data.forEach((datae) => {
                insertRowsAsStream(datae).then((res) => {
                    if (res = 'Ok') {
                        removeDups()
                        .then(data =>{
                        console.log('did it', res);
                        })
                        .catch(e => {
                            console.log(e)
                        })
                    }
                    }).catch((err)=>{
                        console.log(err);
                })  
            })
        })
    })
