import React from 'react';

import {Button}  from 'react-bootstrap';
import RTChart from 'react-rt-chart';
import { Line } from 'react-chartjs';

// import {connectToStores}  from 'fluxible-addons-react';
// import TimeStore from '../stores/TimeStore';
// import updateTime from '../actions/updateTime';

class EntityChart extends React.Component {

    constructor(props) {
        super(props);
        this.state = {data: [], chart: null};
        // for ( var i = 0; i < 10000; i++ ) {
          
        //   y += Math.round(5 + Math.random() * (-5 - 5));    
        //   dataPoints.push({ y: y});
        // }   
    }

    getRandomValue() {
        return Math.random();
    }

    chartRender() {
        var y = 0;
        this.state.chart.render();
    }

    chartLegendClick(e) {
        var y = 0;
        if (typeof(e.dataSeries.visible) === "undefined" || e.dataSeries.visible) {
            e.dataSeries.visible = false;
        } else {
            e.dataSeries.visible = true;
        }
        this.state.chart.render();
    }

    componentDidMount() {
        var chart = new CanvasJS.Chart("chartContainer",
        {
          animationEnabled: true,
          zoomEnabled: true,
          
          title:{
            text: "Top 5 hastags and mentions"
          },    
          data: [
              {
                type: "spline", 
                showInLegend: true,             
                dataPoints: []
              },
              {
                type: "spline",
                showInLegend: true,          
                dataPoints: []
              }
          ],
            legend: {
                cursor: "pointer",
                itemclick: this.chartLegendClick.bind(this)
            }          
        });

        this.setState({chart: chart});

        var that = this;
        setInterval(function() {
            var data = that.state.data;

            var newData0 = { x: new Date(), y: that.getRandomValue() };
            var newData1 = { x: new Date(), y: that.getRandomValue() };

            data.push(newData0);

            that.state.chart.options.data[0].dataPoints.push(newData0);
            that.state.chart.options.data[1].dataPoints.push(newData1);
            that.setState({data: data});
        }, 1000);
    }

    componentDidUpdate() {
        this.chartRender();
    }

    render () {
        var chart = {
            // axis: {
            //     y: { min: 50, max: 100 }
            // },
            point: {
                show: false
            }
        };


        var graphData = this.state.data;

    // var options = {
    //   // responsive: true,
    //   pointDotRadius: 0,
    //   scaleShowHorizontalLines: false,
    //   scaleShowVerticalLines: false,
    //   // showScale: false,
    //   datasetStrokeWidth: 6,

    //   // tooltips
    //   pointHitDetectionRadius: 1,
    //   tooltipTemplate: "",
    //   tooltipEvents: [],
    //   // tooltipTemplate: "<%= value %>%",

    //   // y axis
    //   // scaleOverride: true,
    //   // scaleSteps: 3,
    //   // scaleStepWidth: 50,
    //   // scaleStartValue: 0
    // };

    // var data = {
    //   labels: graphData.map( x => x.date.getDay() % 7 == 0 ? (1 + x[0].getMonth()) + '/' + x[0].getDate() : '' ),
    //   datasets: [
    //     {
    //       fillColor: "#fff",
    //       strokeColor: "#31a4d9",
    //       pointColor: "#31a4d9",
    //       pointStrokeColor: "#fff",
    //       pointHighlightFill: "#fff",
    //       pointHighlightStroke: "#31a4d9",
    //       data: graphData.map( x => x.Car )
    //     }
    //   ]
    // };

    return <div id="chartContainer" style={{height: "300px", width: "100%"}}></div>

    // return <Line data={data} options={options} />

        // return <RTChart
        //         chart={chart}
        //         fields={['Car','Bus']}
        //         maxValues={1000}
        //         initialData={data} />
    }
}

// Timestamp.contextTypes = {
//     executeAction: React.PropTypes.func.isRequired
// };

// Timestamp = connectToStores(Timestamp, [TimeStore], (context, props) => {
//     return context.getStore(TimeStore).getState();
// });

export default EntityChart;
