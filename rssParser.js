let Parser = require('rss-parser');
let moment = require('moment');
const {
    BigQuery
} = require('@google-cloud/bigquery');

let parser = new Parser();
const urls = [{
        program: "Julie's Library",
        feed: "https://feeds.publicradio.org/public_feeds/julies-library/rss/rss"
    },
    {
        program: "Don't Ask Tig",
        feed: "https://feeds.publicradio.org/public_feeds/dont-ask-tig/rss/rss.rss"
    },
    {
        program: "In the Dark",
        feed: "https://feeds.publicradio.org/public_feeds/in-the-dark/rss/rss"
    },
    {
        program: "Terrible, Thanks for Asking",
        feed: "https://feeds.publicradio.org/public_feeds/terrible-thanks-for-asking/rss/rss.rss"
    },
    {
        program: "The Hilarious World of Depression",
        feed: "https://feeds.publicradio.org/public_feeds/the-hilarious-world-of-depression/rss/rss.rss"
    },
    {
        program: "Spectacular Failures",
        feed: "https://feeds.publicradio.org/public_feeds/spectacular-failures/rss/rss.rss"
    },
    {
        program: "The Slowdown",
        feed: "https://feeds.publicradio.org/public_feeds/the-slowdown/rss/rss.rss"
    },
    {
        program: "The Splendid Table",
        feed: "https://feeds.publicradio.org/public_feeds/the-splendid-table/rss/rss.rss"
    },
    {
        program: "Brains On!",
        feed: "https://feeds.publicradio.org/public_feeds/brains-on/rss/rss.rss"
    },
    {
        program: "Field Work",
        feed: "https://feeds.publicradio.org/public_feeds/fieldwork/rss/rss.rss"
    },
    {
        program: "Smash Boom Best",
        feed: "https://feeds.publicradio.org/public_feeds/smash-boom-best/rss/rss.rss"
    },
    {
        program: "TBTL",
        feed: "https://feeds.publicradio.org/public_feeds/tbtl/rss/rss.rss"
    }
];

const bigquery = new BigQuery({
    projectId: `apmg-data-warehouse`
});
const datasetId = 'apm_podcasts';
const tableId = 'episode_titles';
async function insertRowsAsStream(param) {
    console.log('here is dataToAdd', param);
    let rows = param;
    console.log('here is rows! ', rows)
    await bigquery
        .dataset(datasetId)
        .table(tableId)
        .insert(rows);
    console.log(`Inserted ${rows.length} rows`);
    return 'Ok';
}
let removeDups = async () => {
    let sqlQuery = `CREATE OR REPLACE TABLE ${datasetId}.${tableId} AS SELECT Episode, uri_path, Program, Title FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY Program, Episode) row_number FROM  ${datasetId}.${tableId}) WHERE row_number = 1)`;
    const options = {
        query: sqlQuery,
        location: 'US'
    };
    const [rows] = await bigquery.query(options);
    console.log(`Table is now ${rows.length} rows`);
}
    let parseRSS = ()=> {
        return new Promise((resolve, reject) => { 
            let dataToAdd = [];
            urls.forEach(async (url) => {
             let program = await parser.parseURL(url.feed, (err, feed) => {
                 if (err) {
                     reject(err);
                 }
                 if (feed.title = url.program) {
 
                     feed.items.forEach(item => {
                        dataToAdd.push({
                             program: url.program,
                             title: item.title,
                             episode: moment(item.pubDate).format('YYYY-MM-DD'),
                             uri_path: '/podcast' + item.guid
                         })
 
                     });
                 }
                 resolve(dataToAdd);
             });
             return program;
         })
    
     });
    }  








module.exports = (async () => {
    let dataToAdd = await parseRSS();
        console.log('got the data', dataToAdd);
        insertRowsAsStream(dataToAdd).then((res) => {
            if (res = 'Ok') {
                removeDups()
                .then(data =>{
                    console.log('all done', data);
                })
                .catch(e => {
                    console.log(e)
                })
                
            }
        }).catch((err)=>{
            console.log(err);
        })
});
    