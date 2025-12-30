import dotenv from "dotenv";
dotenv.config();
import { BigQuery } from "@google-cloud/bigquery";
import anyAscii from "any-ascii";
import Parser from "rss-parser";
import moment from "moment";
import base64 from "base-64";
import fetch from "node-fetch";
import { Headers } from "node-fetch";

// Function to get JWT token
async function getJWTToken() {
  const authHeaders = new Headers();
  authHeaders.append(
    "Authorization",
    `Basic ${base64.encode(`${process.env.username}:${process.env.password}`)}`
  );
  authHeaders.append("Content-Type", "application/json");
  
  const authOptions = {
    method: "POST",
    headers: authHeaders,
    body: JSON.stringify({
      grant_type: "client_credentials"
    }),
    redirect: "follow",
  };

  try {
    const response = await fetch(`${process.env.AUTH_ENDPOINT}/authenticate`, authOptions);
    const tokenData = await response.json();
    
    if (!response.ok) {
      throw new Error(`Authentication failed: ${tokenData.error || response.statusText}`);
    }
    
    return tokenData.token;
  } catch (error) {
    console.error('Failed to obtain JWT token:', error);
    throw error;
  }
}
const parser = new Parser();
const bigquery = new BigQuery({ projectId: "apmg-data-warehouse" });
const datasetId = "apm_podcasts";
const tableId = "episode_legend_stage";
const urls = [
    {
    feed: "https://feeds.publicradio.org/public_feeds/financially-inclined",
    program: "Financially Inclined",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/how-we-survive",
    program: "How We Survive",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/make-me-smart",
    program: "Make Me Smart",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/marketplace",
    program: "Marketplace",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/marketplace-morning-report",
    program: "Marketplace Morning Report",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/marketplace-tech",
    program: "Marketplace Tech",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/million-bazillion",
    program: "Million Bazillion",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/sold-a-story",
    program: "Sold a Story",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/the-slowdown",
    program: "The Slowdown",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/this-is-uncomfortable",
    program: "This Is Uncomfortable",
  },
  {
    feed: "https://feeds.publicradio.org/public_feeds/this-old-house-radio-hour",
    program: "This Old House Radio Hour"
  }
];

let adjustPubDate = function (program, date) {
  if (program === "Marketplace" || program === "Make Me Smart") {
    return moment(date).subtract(6, "hours").format("YYYY-MM-DD");
  } else {
    return moment(date).format("YYYY-MM-DD");
  }
};

async function callTriton() {
  return new Promise(async (resolve, reject) => {
    try {
      // Get JWT token before making the API request
      const jwtToken = await getJWTToken();
      
      // Create headers with JWT token
      const headers = new Headers();
      headers.append("Authorization", `Bearer ${jwtToken}`);
      headers.append("Content-Type", "application/json");
      
      const options = {
        method: "GET",
        headers: headers,
        redirect: "follow",
      };

      let dataToAdd = [];
      function createRecord(item) {
        return {
          program: item[0].exportValue,
          title: anyAscii(item[3].exportValue),
        };
      }

      const res = await fetch(
        "https://metrics.api.tritondigital.com/reports/4e082a21-6a9b-4dec-b157-1238729a36c9",
        options
      );
      
      if (!res.ok) {
        throw new Error(`API request failed: ${res.status} ${res.statusText}`);
      }
      
      const response = await res.json();
      let episodes = response.data;
      for (let i = 0; i < episodes.length; i++) {
        var obj = createRecord(episodes[i]);
        dataToAdd.push(obj);
      }
      resolve(dataToAdd);
      
    } catch (error) {
      console.log(error);
      reject(error);
    }
  });
}
// Function to test RSS feed accessibility
async function testRSSFeed(url) {
  try {
    const response = await fetch(url.feed, { method: 'HEAD' });
    return {
      program: url.program,
      feed: url.feed,
      accessible: response.ok,
      status: response.status,
      statusText: response.statusText
    };
  } catch (error) {
    return {
      program: url.program,
      feed: url.feed,
      accessible: false,
      status: null,
      statusText: error.message
    };
  }
}

async function dissectRSS(url) {
  return new Promise((resolve, reject) => {
    console.log(`Processing RSS feed: ${url.program} - ${url.feed}`);
    const parseUri = /\/o[^\/]*(\/[^?]*)/;
    parser.parseURL(url.feed, (err, feed) => {
      if (err) {
        console.error(`Failed to parse RSS feed for ${url.program} (${url.feed}):`, err.message);
        reject(new Error(`RSS Feed Error for ${url.program}: ${err.message}`));
      } else {
        const dataToAdd = feed.items
          .filter((item) => item.hasOwnProperty("enclosure"))
          .map((item) => ({
            program: url.program,
            episode: adjustPubDate(url.program, item.pubDate),
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
// Diagnostic function to check all RSS feeds
export async function checkAllRSSFeeds() {
  console.log('Checking accessibility of all RSS feeds...\n');
  const feedTests = await Promise.all(urls.map(url => testRSSFeed(url)));
  
  feedTests.forEach(test => {
    const status = test.accessible ? '✅ ACCESSIBLE' : '❌ INACCESSIBLE';
    console.log(`${status} - ${test.program}`);
    console.log(`  URL: ${test.feed}`);
    if (!test.accessible) {
      console.log(`  Error: ${test.status || 'Network Error'} - ${test.statusText}`);
    }
    console.log('');
  });
  
  const accessible = feedTests.filter(test => test.accessible).length;
  const total = feedTests.length;
  console.log(`Summary: ${accessible}/${total} feeds are accessible`);
  
  return feedTests;
}

export async function processAndMergeData() {
  try {
    console.log('Starting data processing...');
    
    // Test RSS feed accessibility first
    console.log('Testing RSS feed accessibility...');
    const feedTests = await Promise.all(urls.map(url => testRSSFeed(url)));
    const inaccessibleFeeds = feedTests.filter(test => !test.accessible);
    
    if (inaccessibleFeeds.length > 0) {
      console.warn('The following feeds are not accessible:');
      inaccessibleFeeds.forEach(feed => {
        console.warn(`- ${feed.program}: ${feed.feed} (Status: ${feed.status || 'Error'} - ${feed.statusText})`);
      });
    }
    
    const tritonData = await callTriton();
    
    // Process RSS feeds with individual error handling
    const rssPromises = urls.map(async (url) => {
      try {
        return await dissectRSS(url);
      } catch (error) {
        console.error(`Skipping ${url.program} due to error: ${error.message}`);
        return []; // Return empty array for failed feeds
      }
    });
    
    const rssResults = await Promise.all(rssPromises);
    const rssData = rssResults.flat();
    
    console.log(`Successfully processed ${rssData.length} RSS items`);
    
    const startDate = moment().subtract(10, "days").format("YYYY-MM-DD");
    const filteredRssData = rssData.filter((item) => item.episode >= startDate);
    const mergedData = await findMin(filteredRssData, tritonData, "title");
    
    if (mergedData.length > 0) {
      await insertRowsAsStream(mergedData);
      return `Process completed successfully. Processed ${mergedData.length} merged records.`;
    } else {
      return "Process completed but no data to merge.";
    }
  } catch (error) {
    console.error("Error processing data: ", error);
    throw error;
  }
}
