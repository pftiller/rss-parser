let Parser = require('rss-parser');
let moment = require('moment');
const {BigQuery} = require('@google-cloud/bigquery');
let parser = new Parser();
const urls = [
    {
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
    },
    {
        program: "TBTL",
        feed: "https://feeds.publicradio.org/public_feeds/tbtl/rss/rss.rss"
    },
    {
        program: "In Front of Our Eyes",
        feed: "https://feeds.publicradio.org/public_feeds/in-front-of-our-eyes/rss/rss.rss"
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
        let parseUri = new RegExp('rss\/o(.*)');
        function createRecord(url, item) {
            return {
                program: url.program,
                title: item.title,
                uri_path: null,
                episode: moment(item.pubDate).format('YYYY-MM-DD'),
                getUri() {
                    return this.uri_path = parseUri.exec(item.enclosure.url);
                }
            }
        }
        parser.parseURL(url.feed, (err, feed) => {
            if (err) {
                reject(err);
            } else if (feed.title = url.program) {
                feed.items.forEach(item => {
                    if (item.hasOwnProperty('enclosure')) {
                        var obj = createRecord(url, item);
                        var obj2 = obj.getUri();
                        obj.uri_path = obj2[1];
                        delete obj.getUri;
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
