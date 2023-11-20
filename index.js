import dotenv from "dotenv";
dotenv.config();
import { BigQuery } from "@google-cloud/bigquery";
import anyAscii from 'any-ascii';
import Parser from "rss-parser";
import moment from "moment";
import base64 from "base-64";
import fetch from "node-fetch";
import { Headers } from "node-fetch";
const headers = new Headers();
headers.append(
  "Authorization",
  `Basic ${base64.encode(`${process.env.username}:${process.env.password}`)}`
);
const options = {
  method: "GET",
  headers: headers,
  redirect: "follow",
};
const parser = new Parser();
const bigquery = new BigQuery({ projectId: "apmg-data-warehouse" });
const datasetId = "apm_podcasts";
const tableId = "episode_legend_stage";
const urls = [
  {
    feed: "https://feeds.publicradio.org/public_feeds/brains-on/rss/rss.rss",
    program: "Brains On! Science podcast for kids",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/charm-words/rss/rss.rss",
    program: "Charm Words: Daily Affirmations for Kids",
  },
  {
    feed: "https://www.marketplace.org/feed/podcast/corner-office-from-marketplace",
    program: "Corner Office from Marketplace",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/dont-ask-tig/rss/rss.rss",
    program: "Don't Ask Tig",
  },
  {
    feed: "https://www.marketplace.org/feed/podcast/financially-inclined",
    program: "Financially Inclined",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/forever-ago/rss/rss.rss",
    program: "Forever Ago",
  },
  {
    feed: "https://www.marketplace.org/feed/podcast/how-we-survive",
    program: "How We Survive",
  },
  {
    feed: "https://www.marketplace.org/feed/podcast/make-me-smart",
    program: "Make Me Smart",
  },
  {
    feed: "https://www.marketplace.org/feed/podcast/marketplace",
    program: "Marketplace",
  },
  {
    feed: "https://www.marketplace.org/feed/podcast/marketplace-morning-report",
    program: "Marketplace Morning Report",
  },
  {
    feed: "https://www.marketplace.org/feed/podcast/marketplace-tech",
    program: "Marketplace Tech",
  },
  {
    feed: "https://www.marketplace.org/feed/podcast/million-bazillion",
    program: "Million Bazillion",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/moment-of-um/rss/rss.rss",
    program: "Moment of Um",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/sent-away/rss/rss.rss",
    program: "Sent Away",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/smash-boom-best/rss/rss.rss",
    program: "Smash Boom Best",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/sold-a-story/rss/rss.rss",
    program: "Sold a Story",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/tiny-huge-decisions/rss/rss.rss",
    program: "Tiny Huge Decisions",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/angry-therapist/rss/rss.rss",
    program: "The Angry Therapist",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/the-one-recipe/rss/rss.rss",
    program: "The One Recipe",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/the-slowdown/rss/rss.rss",
    program: "The Slowdown",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/the-splendid-table/rss/rss.rss",
    program: "The Splendid Table",
  },
  {
    feed: "https://www.marketplace.org/feed/podcast/the-uncertain-hour",
    program: "The Uncertain Hour",
  },
  {
    feed: "https://www.marketplace.org/feed/podcast/this-is-uncomfortable-reema-khrais",
    program: "This Is Uncomfortable",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/truth-be-told/rss/rss.rss",
    program: "Truth Be Told with Tonya Mosley",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/classical-kids-storytime/rss/rss.rss",
    program: "YourClassical Storytime",
  },
];

async function callTriton() {
  return new Promise((resolve, reject) => {
    let dataToAdd = [];
    function createRecord(item) {
      return {
        program: item[0].exportValue,
        title: anyAscii(item[3].exportValue),
      };
    }

    fetch(
      "https://metrics-api.tritondigital.com/v1/podcast/saved-query/4e082a21-6a9b-4dec-b157-1238729a36c9/",
      options
    )
      .then((res) => res.json())
      .then((response) => {
        let episodes = response.data;
        for (let i = 0; i < episodes.length; i++) {
          var obj = createRecord(episodes[i]);
          dataToAdd.push(obj);
        }
        resolve(dataToAdd);
      })
      .catch((error) => {
        console.log(error);
        reject(error);
      });
  });
}
async function dissectRSS(url) {
  return new Promise((resolve, reject) => {
    const dataToAdd = [];
    const parseUri = /\/o(,?.*)/;
    function createRecord(url, item) {
      return {
        program: url.program,
        episode: moment(item.pubDate).subtract(6, "hours").format("YYYY-MM-DD"),
        title: anyAscii(item.title),
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
        });
      }
    });
    resolve(dataToAdd);
  });
}

async function insertRowsAsStream(param) {
  const rows = param;
  try {
    await bigquery.dataset(datasetId).table(tableId).insert(rows);
    return "Ok";
  } catch (error) {
    console.error("received error", error);
  }
}
function mergeObjects(obj, src) {
  return { ...obj, ...src };
}
async function findMin(rss, triton) {
  let dataToSend = [];
  let minimum = Math.min(rss.length, triton.length);
  for (let i = 0; i < minimum; i++) {
    const mergedObject = mergeObjects(triton[i], rss[i]);
    dataToSend.push(mergedObject);
  }
  return dataToSend;
}
export async function processAndMergeData() {
  try {
    const tritonData = await callTriton();
    const rssPromises = urls.map((url) => dissectRSS(url));
    const rssData = await Promise.all(rssPromises);
    const mergedData = await findMin(rssData, tritonData);
    await insertRowsAsStream(mergedData);
    return "Process completed successfully.";
  } catch (error) {
    console.error("Error processing data: ", error);
  }
}
