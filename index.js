import Parser from 'rss-parser';
import moment from 'moment';
import {BigQuery} from '@google-cloud/bigquery';
let parser = new Parser();
const urls = [
    {
      "feed": "https://feeds.publicradio.org/public_feeds/brains-on/rss/rss.rss",
      "program": "Brains On!"
    },
    {
      "feed": "https://feeds.publicradio.org/public_feeds/charm-words/rss/rss.rss",
      "program": "Charm Words"
    },
    {
      "feed": "https://www.marketplace.org/feed/podcast/corner-office-from-marketplace",
      "program": "Corner Office"
    },
    {
      "feed": "https://feeds.publicradio.org/public_feeds/dont-ask-tig/rss/rss.rss",
      "program": "Don't Ask Tig"
    },
    {
      "feed": "https://www.marketplace.org/feed/podcast/financially-inclined",
      "program": "Financially Inclined"
    },
    {
      "feed": "https://feeds.publicradio.org/public_feeds/forever-ago/rss/rss.rss",
      "program": "Forever Ago"
    },
    {
      "feed": "https://www.marketplace.org/feed/podcast/how-we-survive",
      "program": "How We Survive"
    },
    {
      "feed": "https://www.marketplace.org/feed/podcast/make-me-smart",
      "program": "Make Me Smart"
    },
    {
      "feed": "https://www.marketplace.org/feed/podcast/marketplace",
      "program": "Marketplace PM"
    },
    {
      "feed": "https://www.marketplace.org/feed/podcast/marketplace-morning-report",
      "program": "Marketplace Morning Report"
    },
    {
      "feed": "https://www.marketplace.org/feed/podcast/marketplace-tech",
      "program": "Marketplace Tech Report"
    },
    {
      "feed": "https://www.marketplace.org/feed/podcast/million-bazillion",
      "program": "Million Bazillion"
    },
    {
      "feed": "https://feeds.publicradio.org/public_feeds/moment-of-um/rss/rss.rss",
      "program": "Moment of Um"
    },
    {
      "feed": "https://feeds.publicradio.org/public_feeds/sent-away/rss/rss.rss",
      "program": "Sent Away"
    },
    {
      "feed": "https://feeds.publicradio.org/public_feeds/smash-boom-best/rss/rss.rss",
      "program": "Smash Boom Best"
    },
    {
      "feed": "https://feeds.publicradio.org/public_feeds/sold-a-story/rss/rss.rss",
      "program": "Sold a Story"
    },
    {
      "feed": "https://feeds.publicradio.org/public_feeds/tiny-huge-decisions/rss/rss.rss",
      "program": "Tiny Huge Decisions"
    },
    {
      "feed": "https://feeds.publicradio.org/public_feeds/angry-therapist/rss/rss.rss",
      "program": "The Angry Therapist"
    },
    {
      "feed": "https://feeds.publicradio.org/public_feeds/the-one-recipe/rss/rss.rss",
      "program": "The One Recipe"
    },
    {
      "feed": "https://feeds.publicradio.org/public_feeds/the-slowdown/rss/rss.rss",
      "program": "The Slowdown"
    },
    {
      "feed": "https://feeds.publicradio.org/public_feeds/the-splendid-table/rss/rss.rss",
      "program": "The Splendid Table"
    },
    {
      "feed": "https://www.marketplace.org/feed/podcast/the-uncertain-hour",
      "program": "The Uncertain Hour"
    },
    {
      "feed": "https://www.marketplace.org/feed/podcast/this-is-uncomfortable-reema-khrais",
      "program": "This is Uncomfortable"
    },
    {
      "feed": "https://feeds.publicradio.org/public_feeds/truth-be-told/rss/rss.rss",
      "program": "Truth Be Told with Tonya Mosley"
    },
    {
      "feed": "https://feeds.publicradio.org/public_feeds/classical-kids-storytime/rss/rss.rss",
      "program": "YourClassical Storytime"
    }
  ];
const projectId = `apmg-data-warehouse`;
const bigquery = new BigQuery({
    projectId: projectId
});
const datasetId = "apm_podcasts";
const tableId = "episode_titles";
async function insertRowsAsStream(param) {
    let rows = param;
    await bigquery
        .dataset(datasetId)
        .table(tableId)
        .insert(rows);
    console.log(`Inserted ${rows.length} rows`);
    return "Ok";
}
let removeDups = async () => {
    let sqlQuery = `CREATE OR REPLACE TABLE ${projectId}.${datasetId}.${tableId} AS SELECT Episode, uri_path, Program, Title FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY Program, Episode, uri_path) row_number FROM ${projectId}.${datasetId}.${tableId} ) WHERE row_number = 1`;
    const options = {
        query: sqlQuery,
        location: "US"
    };
    const [rows] = await bigquery.query(options);
    console.log(`Table is now ${rows.length} rows`);
};
let dissectRSS = (url) => {
    return new Promise((resolve, reject) => {
        let dataToAdd = [];
        let parseUri = new RegExp("\/o(,?.*)");
        function createRecord(url, item) {
            return {
                episode: moment(item.pubDate).format("YYYY-MM-DD"),
                program: url.program,
                title: item.title,
                uri_path: parseUri.exec(item.enclosure.url)[1]
            };
        }
        parser.parseURL(url.feed, (err, feed) => {
            if (err) {
                reject(err);
            } else if (feed.title = url.program) {
                feed.items.forEach((item) => {
                    if (item.hasOwnProperty("enclosure")) {
                        var obj = createRecord(url, item);
                        dataToAdd.push(obj);
                    }
                    resolve(dataToAdd);
                });
            }
        });
    });
};
let dataArray = [];
urls.forEach(async (url) => {
    let feed = dissectRSS(url);
    dataArray.push(feed);
});
export function parseRss() {
    Promise.all(dataArray).then((data) => {
        console.log("got the data", data);
        data.forEach((datae) => {
            insertRowsAsStream(datae).then((res) => {
                if (res = "Ok") {
                    removeDups()
                        .then((data) => {
                            console.log("did it", res);
                        })
                        .catch((e) => {
                            console.log(e);
                        });
                }
            }).catch((err) => {
                console.log(err);
            });
        });
    });
}