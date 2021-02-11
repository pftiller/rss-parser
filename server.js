// const cron = require('node-cron');
const express = require("express");
const app = express();
const parseRss = require('./rssParser');


const PORT = process.env.PORT || 3001;

cron.schedule('0 2 * * *', function() {
    console.log('running the funtion');
    parseRss()
  });

app.listen(PORT, (error) => {
      console.log("listening on " + PORT + "...");
  });
