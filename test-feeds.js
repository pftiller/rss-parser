import { checkAllRSSFeeds } from './index.js';

// Test all RSS feeds to identify 404 issues
checkAllRSSFeeds().then(results => {
  console.log('Feed test completed');
}).catch(error => {
  console.error('Error testing feeds:', error);
});