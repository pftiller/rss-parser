import Parser from 'rss-parser'
import moment from 'moment'
import { BigQuery } from '@google-cloud/bigquery'
const parser = new Parser()
const urls = [
  {
    feed: 'https://feeds.publicradio.org/public_feeds/brains-on/rss/rss.rss',
    program: 'Brains On!'
  },
  {
    feed: 'https://feeds.publicradio.org/public_feeds/charm-words/rss/rss.rss',
    program: 'Charm Words'
  },
  {
    feed: 'https://www.marketplace.org/feed/podcast/corner-office-from-marketplace',
    program: 'Corner Office'
  },
  {
    feed: 'https://feeds.publicradio.org/public_feeds/dont-ask-tig/rss/rss.rss',
    program: "Don't Ask Tig"
  },
  {
    feed: 'https://www.marketplace.org/feed/podcast/financially-inclined',
    program: 'Financially Inclined'
  },
  {
    feed: 'https://feeds.publicradio.org/public_feeds/forever-ago/rss/rss.rss',
    program: 'Forever Ago'
  },
  {
    feed: 'https://www.marketplace.org/feed/podcast/how-we-survive',
    program: 'How We Survive'
  },
  {
    feed: 'https://www.marketplace.org/feed/podcast/make-me-smart',
    program: 'Make Me Smart'
  },
  {
    feed: 'https://www.marketplace.org/feed/podcast/marketplace',
    program: 'Marketplace PM'
  },
  {
    feed: 'https://www.marketplace.org/feed/podcast/marketplace-morning-report',
    program: 'Marketplace Morning Report'
  },
  {
    feed: 'https://www.marketplace.org/feed/podcast/marketplace-tech',
    program: 'Marketplace Tech Report'
  },
  {
    feed: 'https://www.marketplace.org/feed/podcast/million-bazillion',
    program: 'Million Bazillion'
  },
  {
    feed: 'https://feeds.publicradio.org/public_feeds/moment-of-um/rss/rss.rss',
    program: 'Moment of Um'
  },
  {
    feed: 'https://feeds.publicradio.org/public_feeds/sent-away/rss/rss.rss',
    program: 'Sent Away'
  },
  {
    feed: 'https://feeds.publicradio.org/public_feeds/smash-boom-best/rss/rss.rss',
    program: 'Smash Boom Best'
  },
  {
    feed: 'https://feeds.publicradio.org/public_feeds/sold-a-story/rss/rss.rss',
    program: 'Sold a Story'
  },
  {
    feed: 'https://feeds.publicradio.org/public_feeds/tiny-huge-decisions/rss/rss.rss',
    program: 'Tiny Huge Decisions'
  },
  {
    feed: 'https://feeds.publicradio.org/public_feeds/angry-therapist/rss/rss.rss',
    program: 'The Angry Therapist'
  },
  {
    feed: 'https://feeds.publicradio.org/public_feeds/the-one-recipe/rss/rss.rss',
    program: 'The One Recipe'
  },
  {
    feed: 'https://feeds.publicradio.org/public_feeds/the-slowdown/rss/rss.rss',
    program: 'The Slowdown'
  },
  {
    feed: 'https://feeds.publicradio.org/public_feeds/the-splendid-table/rss/rss.rss',
    program: 'The Splendid Table'
  },
  {
    feed: 'https://www.marketplace.org/feed/podcast/the-uncertain-hour',
    program: 'The Uncertain Hour'
  },
  {
    feed: 'https://www.marketplace.org/feed/podcast/this-is-uncomfortable-reema-khrais',
    program: 'This is Uncomfortable'
  },
  {
    feed: 'https://feeds.publicradio.org/public_feeds/truth-be-told/rss/rss.rss',
    program: 'Truth Be Told with Tonya Mosley'
  },
  {
    feed: 'https://feeds.publicradio.org/public_feeds/classical-kids-storytime/rss/rss.rss',
    program: 'YourClassical Storytime'
  }
]
const projectId = 'apmg-data-warehouse'
const bigquery = new BigQuery({
  projectId
})
const datasetId = 'apm_podcasts'
const tableId = 'episode_titles'
async function insertRowsAsStream (param) {
  const rows = param
  await bigquery.dataset(datasetId).table(tableId).insert(rows)
  console.log(`Inserted ${rows.length} rows`)
  return 'Ok'
}
const dissectRSS = (url) => {
  return new Promise((resolve, reject) => {
    const dataToAdd = []
    const parseUri = /\/o(,?.*)/
    function createRecord (url, item) {
      return {
        episode: moment(item.pubDate).format('YYYY-MM-DD'),
        program: url.program,
        title: item.title,
        uri_path: null,
        getUri () {
          this.uri_path = parseUri.exec(item.enclosure.url)
          return this.uri_path
        }
      }
    }
    parser.parseURL(url.feed, (err, feed) => {
      if (err) {
        reject(err)
      } else if ((feed.title = url.program)) {
        feed.items.forEach((item) => {
          if (Object.prototype.hasOwnProperty.call(item, 'enclosure')) {
            const obj = createRecord(url, item)
            const obj2 = obj.getUri()
            obj.uri_path = obj2[1]
            delete obj.getUri
            dataToAdd.push(obj)
          }
          resolve(dataToAdd)
        })
      }
    })
  })
}
const dataArray = []
urls.forEach(async (url) => {
  const feed = dissectRSS(url)
  dataArray.push(feed)
})
export function parseRss () {
  Promise.all(dataArray).then((data) => {
    console.log('got the data', data)
    data.forEach((datae) => {
      insertRowsAsStream(datae)
        .then((res) => {
          if (res === 'Ok') {
            console.log('did it', res)
          }
        })
        .catch((err) => {
          console.log(err)
        })
    })
  })
}
