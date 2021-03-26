const express = require("express");
const app = express();
const parseRss = require('./rssParser');


const PORT = process.env.PORT || 3001;

parseRss();

app.listen(PORT, (error) => {
      console.log("listening on " + PORT + "...");
  });
