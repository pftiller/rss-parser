const express = require('express');
const router = express.Router();
let Parser = require('rss-parser');
let moment = require('moment');
const {BigQuery} = require('@google-cloud/bigquery');
let parser = new Parser();
const urls = [
    {program: "Julie's Library", feed: "https://feeds.publicradio.org/public_feeds/julies-library/rss/rss"},
    {program: "Don't Ask Tig", feed: "https://feeds.publicradio.org/public_feeds/dont-ask-tig/rss/rss.rss"},
    {program: "In the Dark", feed: "https://feeds.publicradio.org/public_feeds/in-the-dark/rss/rss"},
    {program: "Terrible, Thanks for Asking", feed: "https://feeds.publicradio.org/public_feeds/terrible-thanks-for-asking/rss/rss.rss"},
    {program: "The Hilarious World of Depression", feed: "https://feeds.publicradio.org/public_feeds/the-hilarious-world-of-depression/rss/rss.rss"},
    {program: "Spectacular Failures", feed: "https://feeds.publicradio.org/public_feeds/spectacular-failures/rss/rss.rss"},
    {program: "The Slowdown", feed: "https://feeds.publicradio.org/public_feeds/the-slowdown/rss/rss.rss"},
    {program: "The Splendid Table", feed: "https://feeds.publicradio.org/public_feeds/the-splendid-table/rss/rss.rss"},
    {program: "Brains On!", feed: "https://feeds.publicradio.org/public_feeds/brains-on/rss/rss.rss"},
    {program: "Field Work", feed: "https://feeds.publicradio.org/public_feeds/fieldwork/rss/rss.rss"},
    {program: "Smash Boom Best", feed: "https://feeds.publicradio.org/public_feeds/smash-boom-best/rss/rss.rss"},
    {program: "TBTL", feed: "https://feeds.publicradio.org/public_feeds/tbtl/rss/rss.rss"}
];

module.exports = router.use('/', async (req, res) => {
    const bigquery = new BigQuery({
        projectId: `apmg-data-warehouse`
    });
    const datasetId = 'apm_podcasts';
    const tableId = 'episode_titles';
    let myPromise = new Promise((resolve, reject) => {

        urls.forEach(async (url) => {
            let dataToAdd = [];
            let feed = await parser.parseURL(url.feed);
            if (feed.title = url.program) {
                feed.items.forEach(item => {
                    dataToAdd.push({
                        title: item.title,
                        episode: moment(item.pubDate).format('YYYY-MM-DD'),
                        uri_path: '/podcast' + item.guid
                    })
                });
                url.episodes = dataToAdd
            }
            resolve();
        })
    });
    let insertIntoBigQuery = new Promise((resolve, reject) => {
        bigquery
            .dataset(datasetId)
            .table(tableId)
            .insert(urls);
        console.log(`Inserted ${urls.length} rows`);
        resolve();
    })
    myPromise.then(() => {
        console.log('rows have been inserted')
        insertIntoBigQuery.then(() => {
            let removeDups = async () => {
                let sqlQuery = `CREATE OR REPLACE TABLE ${datasetId}.${tableId} AS (SELECT Episode, uri_path, Program, Title FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY Program, Episode) row_number FROM  ${datasetId}.${tableId}) WHERE row_number = 1))`;
                const options = {
                    query: sqlQuery,
                    location: 'US'
                };
                const [rows] = await bigquery.query(options);
                console.log(`Table is now ${rows.length} rows`);
            }
            removeDups();
            res.send('done');
        }).catch(e => {
            console.log(e)
        })
    }).catch(e => {
        console.log(e)
    });
});

