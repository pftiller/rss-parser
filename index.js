import * as dotenv from "dotenv";
dotenv.config();
import Parser from "rss-parser";
import moment from "moment";
import request from "request";
import { BigQuery } from "@google-cloud/bigquery";
const parser = new Parser();
const urls = [
  //   {
  //     feed: "https://feeds.publicradio.org/public_feeds/brains-on/rss/rss.rss",
  //     program: "Brains On! Science podcast for kids",
  //   },
  //   {
  //     feed: "https://feeds.publicradio.org/public_feeds/charm-words/rss/rss.rss",
  //     program: "Charm Words",
  //   },
  //   {
  //     feed: "https://www.marketplace.org/feed/podcast/corner-office-from-marketplace",
  //     program: "Corner Office from Marketplace",
  //   },
  //   {
  //     feed: "https://feeds.publicradio.org/public_feeds/dont-ask-tig/rss/rss.rss",
  //     program: "Don't Ask Tig",
  //   },
  {
    feed: "https://www.marketplace.org/feed/podcast/financially-inclined",
    program: "Financially Inclined",
    // },
  },
  //   {
  //     feed: "https://feeds.publicradio.org/public_feeds/forever-ago/rss/rss.rss",
  //     program: "Forever Ago",
  //   },
  //   {
  //     feed: "https://www.marketplace.org/feed/podcast/how-we-survive",
  //     program: "How We Survive",
  //   },
  //   {
  //     feed: "https://www.marketplace.org/feed/podcast/make-me-smart",
  //     program: "Make Me Smart",
  //   },
  //   {
  //     feed: "https://www.marketplace.org/feed/podcast/marketplace",
  //     program: "Marketplace",
  //   },
  //   {
  //     feed: "https://www.marketplace.org/feed/podcast/marketplace-morning-report",
  //     program: "Marketplace Morning Report",
  //   },
  //   {
  //     feed: "https://www.marketplace.org/feed/podcast/marketplace-tech",
  //     program: "Marketplace Tech",
  //   },
  //   {
  //     feed: "https://www.marketplace.org/feed/podcast/million-bazillion",
  //     program: "Million Bazillion",
  //   },
  //   {
  //     feed: "https://feeds.publicradio.org/public_feeds/moment-of-um/rss/rss.rss",
  //     program: "Moment of Um",
  //   },
  //   {
  //     feed: "https://feeds.publicradio.org/public_feeds/sent-away/rss/rss.rss",
  //     program: "Sent Away",
  //   },
  //   {
  //     feed: "https://feeds.publicradio.org/public_feeds/smash-boom-best/rss/rss.rss",
  //     program: "Smash Boom Best",
  //   },
  //   {
  //     feed: "https://feeds.publicradio.org/public_feeds/sold-a-story/rss/rss.rss",
  //     program: "Sold a Story",
  //   },
  //   {
  //     feed: "https://feeds.publicradio.org/public_feeds/tiny-huge-decisions/rss/rss.rss",
  //     program: "Tiny Huge Decisions",
  //   },
  //   {
  //     feed: "https://feeds.publicradio.org/public_feeds/angry-therapist/rss/rss.rss",
  //     program: "The Angry Therapist",
  //   },
  //   {
  //     feed: "https://feeds.publicradio.org/public_feeds/the-one-recipe/rss/rss.rss",
  //     program: "The One Recipe",
  //   },
  //   {
  //     feed: "https://feeds.publicradio.org/public_feeds/the-slowdown/rss/rss.rss",
  //     program: "The Slowdown",
  //   },
  //   {
  //     feed: "https://feeds.publicradio.org/public_feeds/the-splendid-table/rss/rss.rss",
  //     program: "The Splendid Table",
  //   },
  //   {
  //     feed: "https://www.marketplace.org/feed/podcast/the-uncertain-hour",
  //     program: "The Uncertain Hour",
  //   },
  //   {
  //     feed: "https://www.marketplace.org/feed/podcast/this-is-uncomfortable-reema-khrais",
  //     program: "This Is Uncomfortable",
  //   },
  //   {
  //     feed: "https://feeds.publicradio.org/public_feeds/truth-be-told/rss/rss.rss",
  //     program: "Truth Be Told with Tonya Mosley",
  //   },
  //   {
  //     feed: "https://feeds.publicradio.org/public_feeds/classical-kids-storytime/rss/rss.rss",
  //     program: "YourClassical Storytime",
  //   },
];
const projectId = `apmg-data-warehouse`;
const bigquery = new BigQuery({
  projectId: projectId,
});
const datasetId = "apm_podcasts";
const tableId = "episode_legend_stage";
async function insertRowsAsStream(param) {
  console.log("here is param", param);
  // const rows = param;
  // await bigquery.dataset(datasetId).table(tableId).insert(rows);
  // console.log(`Inserted ${rows.length} rows`);
  return "Ok";
}
let mergeObjects = (obj, src) => {
  for (var key in src) {
    if (src.hasOwnProperty(key)) obj[key] = src[key];
  }

  return obj;
};

let callTriton = () => {
  return new Promise((resolve, reject) => {
    let yesterdaysEpisodes = [];
    function createRecord(item) {
      return {
        program: item[0].value,
        episode: moment(item[2].exportValue)
          .subtract(6, "hours")
          .format("YYYY-MM-DD"),
        // title: item[3].exportValue,
        title: item[1].exportValue,
      };
    }
    request(
      {
        method: "GET",
        url: "https://metrics-api.tritondigital.com/v1/podcast/saved-query/acd5928a-dc6e-4001-8007-0a4566058f65/",
        // url: "https://metrics-api.tritondigital.com/v1/podcast/saved-query/4e082a21-6a9b-4dec-b157-1238729a36c9/",
        headers: {
          Authorization:
            "Basic dHJpdG9uYXBpYWNjZXNzQG1wci5vcmc6OUw5a2x6enpTNg==",
        },
      },
      function (error, response) {
        if (error) {
          reject(error);
        } else {
          let respArray = JSON.parse(response.body);
          let episodes = respArray.data;
          for (let i = 0; i < episodes.length; i++) {
            var obj = createRecord(episodes[i]);
            yesterdaysEpisodes.push(obj);
          }
        }
        resolve(yesterdaysEpisodes);
      }
    );
  });
};

const dissectRSS = (url) => {
  return new Promise((resolve, reject) => {
    const dataToAdd = [];
    const parseUri = /\/o(,?.*)/;
    function createRecord(url, item) {
      return {
        program: url.program,
        episode: moment(item.pubDate).subtract(6, "hours").format("YYYY-MM-DD"),
        title: item.title,
        uri_path: parseUri.exec(item.enclosure.url)[1],
      };
    }
    parser.parseURL(url.feed, (err, feed) => {
      if (err) {
        reject(err);
      } else if ((feed.title = url.program)) {
        feed.items.forEach((item) => {
          if (item.hasOwnProperty("enclosure")) {
            const obj = createRecord(url, item);
            dataToAdd.push(obj);
          }
          resolve(dataToAdd);
        });
      }
    });
  });
};
const dataArray = [];
urls.forEach(async (url) => {
  const feed = dissectRSS(url);
  dataArray.push(feed);
});
export async function parseRss() {
  let tritonData = await callTriton();
  await Promise.all(dataArray)
    .then((data) => {
      let rssData = data;
      let minimum = Math.min(rssData.length, tritonData.length);
      for (let i = 0; i < minimum; i++) {
          let mergedResult = mergeObjects(rssData[i], tritonData[i]);
          for (let i = 0; i < mergedResult.length; i++) {
            if (mergedResult[i].uri_path !== null) {
              insertRowsAsStream(mergedResult[i])
                .then((res) => {
                  if (res === "Ok") {
                    console.log("did it", res);
                  }
                })
                .catch((err) => {
                  console.log(err);
                });
            }
          }
      }
    })
    .catch((err) => {
      console.log(err);
    });
}
