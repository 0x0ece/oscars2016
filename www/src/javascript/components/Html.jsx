import React, {PropTypes} from 'react';

class Html extends React.Component{
    render () {
        return (
            <html>
                <head>
                    <meta charSet="utf-8" />
                    <meta httpEquiv="X-UA-Compatible" content="IE=edge" />
                    <title>Oscars 2016 on Twitter</title>
                    <meta name="description" content="" />
                    <meta name="viewport" content="width=device-width, initial-scale=1" />
                    <meta property="og:title" content="Oscars 2016 on Twitter" />
                    <meta property="og:type" content="article" />
                    <meta property="og:url" content="http://oscarsdata.github.io/" />
                    <meta property="og:image" content="http://oscarsdata.github.io/oscars2016.jpg" />
                    <meta property="og:description" content="Insights and trending topics during the #Oscars" />
                    <link rel="stylesheet" href="https://fonts.googleapis.com/css?family=Dosis:400,600" />
                    <link rel="stylesheet" href="./css/main.css" />
                </head>
                <body>
                    <div id="app" dangerouslySetInnerHTML={{__html: this.props.markup}}></div>
                    <script dangerouslySetInnerHTML={{__html: this.props.state}}></script>
                    <script src="./js/canvasjs.min.js"></script>
                    <script src="./js/client.js" defer></script>
                </body>
            </html>
        );
    }
}

Html.contextTypes = {
    // assetUrl: PropTypes.func.isRequired,
    siteUrl: PropTypes.func.isRequired
};

export default Html
