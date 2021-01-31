// let Parser = require('rss-parser');
// let moment = require('moment');
// const {BigQuery} = require('@google-cloud/bigquery');
// let parser = new Parser();
// let urls = [
//     {program: "Julie's Library", feed: "https://feeds.publicradio.org/public_feeds/julies-library/rss/rss"},
//     {program: "In the Dark", feed: "https://feeds.publicradio.org/public_feeds/in-the-dark/rss/rss"},
//     {program: "Terrible, Thanks for Asking", feed: "https://feeds.publicradio.org/public_feeds/terrible-thanks-for-asking/rss/rss.rss"},
//     {program: "The Hilarious World of Depression", feed: "https://feeds.publicradio.org/public_feeds/the-hilarious-world-of-depression/rss/rss.rss"},
//     {program: "Spectacular Failures", feed: "https://feeds.publicradio.org/public_feeds/spectacular-failures/rss/rss.rss"},
//     {program: "The Slowdown", feed: "https://feeds.publicradio.org/public_feeds/the-slowdown/rss/rss.rss"},
//     {program: "The Splendid Table", feed: "https://feeds.publicradio.org/public_feeds/the-splendid-table/rss/rss.rss"},
//     {program: "Brains On!", feed: "https://feeds.publicradio.org/public_feeds/brains-on/rss/rss.rss"},
//     {program: "Field Work", feed: "https://feeds.publicradio.org/public_feeds/fieldwork/rss/rss.rss"},
//     {program: "Smash Boom Best", feed: "https://feeds.publicradio.org/public_feeds/smash-boom-best/rss/rss.rss"},
//     {program: "TBTL", feed: "https://feeds.publicradio.org/public_feeds/tbtl/rss/rss.rss"}
// ];

// app.get('/', async (req, res) => {
//     const bigquery = new BigQuery();
//     async function insertRowsAsStream() {
//         const datasetId = 'apm_podcasts';
//         const tableId = 'episode_titles';
//         urls.forEach(url => {
//             (async () => {
//                 let feed = await parser.parseURL(url.feed);
//                 url.title = feed.items[0].title
//                 url.episode = moment(feed.items[0].pubDate).format('YYYY-MM-DD');
//                 url.uri_path = '/podcast' + feed.items[0].guid
//                 delete url.feed;
//                 console.log(url);
//             })();
//         })
//         await bigquery
//             .dataset(datasetId)
//             .table(tableId)
//             .insert(urls);
//         console.log(`Inserted ${urls.length} rows`);
//         return 'Ok'
//     }
//     insertRowsAsStream().then(async (data) => {
//         if(data='Ok') {

//             let removeDups = await bigquery
//             .startQuery({
//                     destination: table,
//                     query: `select distinct * from ${datasetId}.${tableId}`,
//                     createDisposition: "CREATE_IF_NEEDED",
//                     writeDisposition: "WRITE_TRUNCATE",
//                 }
//             )
//             res.send(removeDups);
//         }
//     });
// })
