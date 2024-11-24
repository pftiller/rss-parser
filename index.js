import dotenv from "dotenv";
dotenv.config();
import { BigQuery } from "@google-cloud/bigquery";
import anyAscii from "any-ascii";
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
    feed: "https://feeds.publicradio.org/public_feeds/brains-on",
    program: "Brains On! Science podcast for kids",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/charm-words",
    program: "Charm Words: Daily Affirmations for Kids",
  },
  {
    feed: "https://www.marketplace.org/feed/podcast/financially-inclined",
    program: "Financially Inclined",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/forever-ago",
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
    feed: "https://feeds.publicradio.org/public_feeds/moment-of-um",
    program: "Moment of Um",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/smash-boom-best",
    program: "Smash Boom Best",
  },
  {
    feed: "https://legacyfeeds.publicradio.org/apm-reports/sold-a-story/rss.xml",
    program: "Sold a Story",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/the-slowdown",
    program: "The Slowdown",
  },
  {
    feed: "https://www.marketplace.org/feed/podcast/this-is-uncomfortable-reema-khrais",
    program: "This Is Uncomfortable",
  }
];

async function adjustPubDate(program, date) {
  if (program === "Marketplace" || program === "Make Me Smart") {
    return moment(date).subtract(1, "hours").format("YYYY-MM-DD");
  }
  else {
    return moment(date).format("YYYY-MM-DD");
  }
}

async function callTriton() {
  return new Promise((resolve, reject) => {
    let dataToAdd = [];
    function createRecord(item) {
      return {
        program: item[0].exportValue,
        title: anyAscii(item[3].exportValue),
        episode: adjustPubDate(item[0].exportValue, item[2].exportValue),
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
    const parseUri = /\/o[^\/]*(\/[^?]*)/;
    parser.parseURL(url.feed, (err, feed) => {
      if (err) {
        reject(err);
      } else {
        const dataToAdd = feed.items
          .filter((item) => item.hasOwnProperty("enclosure"))
          .map((item) => ({
            program: url.program,
            episode: moment(item.pubDate)
              .subtract(6, "hours")
              .format("YYYY-MM-DD"),
            title: anyAscii(item.title),
            uri_path: parseUri.exec(item.enclosure.url)[1].toLowerCase(),
          }));
        resolve(dataToAdd);
      }
    });
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
function mergeObjects(array1, array2, key) {
  return array1
    .filter((a1) => array2.some((a2) => a1[key] === a2[key]))
    .map((a1) => {
      let a2 = array2.find((a2) => a1[key] === a2[key]);
      return { ...a1, ...a2 };
    });
}
async function findMin(rss, triton, key) {
  let smallerArray = rss.length <= triton.length ? rss : triton;
  let largerArray = rss.length > triton.length ? rss : triton;
  let mergedData = [];

  smallerArray.forEach((smallItem) => {
    let match = largerArray.find(
      (largeItem) => smallItem[key] === largeItem[key]
    );
    if (match) {
      mergedData.push({ ...smallItem, ...match });
    }
  });
  return mergedData;
}
export async function processAndMergeData() {
  try {
    const tritonData = await callTriton();
    const rssPromises = await urls.map((url) => dissectRSS(url));
    const rssData = (await Promise.all(rssPromises)).flat();
    const startDate = moment().subtract(7, "days").format("YYYY-MM-DD");
    const filteredRssData = rssData.filter((item) => item.episode >= startDate);
    const mergedData = await findMin(filteredRssData, tritonData, "title");
    await insertRowsAsStream(mergedData);
    return "Process completed successfully.";
  } catch (error) {
    console.error("Error processing data: ", error);
  }
}
