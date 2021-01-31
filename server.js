const express = require("express");
const app = express();
const mainRouter = require('./router');


const PORT = process.env.PORT || 3001;


app.use(express.static("public"));
app.use('/route', mainRouter); 


app.listen(PORT, (error) => {
    console.log("listening on " + PORT + "...");
});
