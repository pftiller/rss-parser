import Parser from "rss-parser";
import moment from "moment";
import {BigQuery} from "@google-cloud/bigquery";
const parser = new Parser();
const urls = [{
		feed: "https://feeds.publicradio.org/public_feeds/brains-on/rss/rss.rss",
		id: "b3809b1a-d8eb-11eb-9594-ff8d32426b1f",
		program: "Brains On! Science podcast for kids"
	},
	{
		feed: "https://feeds.publicradio.org/public_feeds/charm-words/rss/rss.rss",
		id: "1453653",
		program: "Charm Words"
	},
	{
		feed: "https://www.marketplace.org/feed/podcast/corner-office-from-marketplace",
		id: "9dfb917c-d8ec-11eb-82dc-478483ad60ca",
		program: "Corner Office from Marketplace"
	},
	{
		feed: "https://feeds.publicradio.org/public_feeds/dont-ask-tig/rss/rss.rss",
		id: "a33b8688-d8ec-11eb-9594-47c7acb499cb",
		program: "Don't Ask Tig"
	},
	{
		feed: "https://www.marketplace.org/feed/podcast/financially-inclined",
		id: "1424523",
		program: "Financially Inclined"
	},
	{
		feed: "https://feeds.publicradio.org/public_feeds/forever-ago/rss/rss.rss",
		id: "a741f206-04ea-11ec-9d4c-179e2e0aa82b",
		program: "Forever Ago"
	},
	{
		feed: "https://www.marketplace.org/feed/podcast/how-we-survive",
		id: "4ac90870-17be-11ec-9fb9-db104c25036c",
		program: "How We Survive"
	},
	{
		feed: "https://www.marketplace.org/feed/podcast/make-me-smart",
		id: "be89f6e0-d8ec-11eb-b321-c335a536ec66",
		program: "Make Me Smart"
	},
	{
		feed: "https://www.marketplace.org/feed/podcast/marketplace",
		id: "ccc1747c-d8ec-11eb-b5e2-6ba46cb92864",
		program: "Marketplace"
	},
	{
		feed: "https://www.marketplace.org/feed/podcast/marketplace-morning-report",
		id: "c5bb0044-d8ec-11eb-a09a-931a443d4364",
		program: "Marketplace Morning Report"
	},
	{
		feed: "https://www.marketplace.org/feed/podcast/marketplace-tech",
		id: "c95d2948-d8ec-11eb-bfab-87156cf77488",
		program: "Marketplace Tech"
	},
	{
		feed: "https://www.marketplace.org/feed/podcast/million-bazillion",
		id: "d07a7208-d8ec-11eb-8fa6-a3366b4257db",
		program: "Million Bazillion"
	},
	{
		feed: "https://feeds.publicradio.org/public_feeds/moment-of-um/rss/rss.rss",
		id: "2192513e-2856-11ec-954b-7b9363df6c97",
		program: "Moment of Um"
	},
	{
		feed: "https://feeds.publicradio.org/public_feeds/sent-away/rss/rss.rss",
		id: "cefe1d76-3bfc-11ec-a0c9-1b3c5a41e210",
		program: "Sent Away"
	},
	{
		feed: "https://feeds.publicradio.org/public_feeds/smash-boom-best/rss/rss.rss",
		id: "d47b0476-d8ec-11eb-a5a6-eb7d03078ea5",
		program: "Smash Boom Best"
	},
	{
		feed: "https://feeds.publicradio.org/public_feeds/sold-a-story/rss/rss.rss",
		id: "372f0d9c-3bfd-11ec-a91a-63234074389e",
		program: "Sold a Story"
	},
	{
		feed: "https://feeds.publicradio.org/public_feeds/tiny-huge-decisions/rss/rss.rss",
		id: "1491333",
		program: "Tiny Huge Decisions"
	},
	{
		feed: "https://feeds.publicradio.org/public_feeds/angry-therapist/rss/rss.rss",
		id: "1b860306-ae3a-11ed-9403-27bd0cf88f61",
		program: "The Angry Therapist"
	},
	{
		feed: "https://feeds.publicradio.org/public_feeds/the-one-recipe/rss/rss.rss",
		id: "3057d2f8-7256-11ec-9bdc-b3e1f05dd842",
		program: "The One Recipe"
	},
	{
		feed: "https://feeds.publicradio.org/public_feeds/the-slowdown/rss/rss.rss",
		id: "df9e552e-d8ec-11eb-a820-2b3bb9f01b42",
		program: "The Slowdown"
	},
	{
		feed: "https://feeds.publicradio.org/public_feeds/the-splendid-table/rss/rss.rss",
		id: "e31a5374-d8ec-11eb-bfab-5b0369ff83f1",
		program: "The Splendid Table"
	},
	{
		feed: "https://www.marketplace.org/feed/podcast/the-uncertain-hour",
		id: "e655f61a-d8ec-11eb-a5a6-b31d48af82bf",
		program: "The Uncertain Hour"
	},
	{
		feed: "https://www.marketplace.org/feed/podcast/this-is-uncomfortable-reema-khrais",
		id: "e9e60248-d8ec-11eb-8716-27ff4700152c",
		program: "This is Uncomfortable"
	},
	{
		feed: "https://feeds.publicradio.org/public_feeds/truth-be-told/rss/rss.rss",
		id: "86967988-c414-11ed-a252-a32a1f54908b",
		program: "Truth Be Told with Tonya Mosley"
	},
	{
		feed: "https://feeds.publicradio.org/public_feeds/classical-kids-storytime/rss/rss.rss",
		id: "9284cf58-a6f4-11ec-9738-b757d0ec2a87",
		program: "YourClassical Storytime"
	},
];
const projectId = `apmg-data-warehouse`;
const bigquery = new BigQuery({
	projectId: projectId,
});
const datasetId = "apm_podcasts";
const tableId = "episode_legend_v4";
async function insertRowsAsStream(param) {
	const rows = param;
	await bigquery.dataset(datasetId).table(tableId).insert(rows);
	console.log(`Inserted ${rows.length} rows`);
	return "Ok";
}
const dissectRSS = (url) => {
	return new Promise((resolve, reject) => {
		const dataToAdd = [];
		const parseUri = /\/o(,?.*)/;

		function createRecord(url, item) {
			return {
				episode_id: url.id + String(moment(item.pubDate).format("YYYY-MM-DD")),
				podcast_id: url.id,
				title: item.title,
				uri_path: parseUri.exec(item.enclosure.url)[1],
				episode: moment(item.pubDate).format("YYYY-MM-DD"),
				program: url.program
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
export function parseRss() {
	Promise.all(dataArray)
		.then((data) => {
			data.forEach((datae) => {
				if (datae.title !== null) {
					insertRowsAsStream(datae)
						.then((res) => {
							if (res === "Ok") {
								console.log("did it", res);
							}
						})
						.catch((err) => {
							console.log(err);
						});
				}
			});
		})
		.catch((err) => {
			console.log(err);
		});
}