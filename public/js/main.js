window.onload = function() {
    if (window.jQuery) {  
        console.log('All set');
        $("#getFeeds").click(function () {
            $.ajax({
                    url: '/route',
                    type: 'GET'
                })
                .then((response) => {

                    var html = `Here was the response: ${response}`;
                    $("#results").append(html);
                });
        });
    } else {
     location.reload();
    }
 }