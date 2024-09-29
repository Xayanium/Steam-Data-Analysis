
/*
 Template Name: Zegva - Responsive Bootstrap 4 Admin Dashboard
 Author: Themesdesign
 Website: www.themesdesign.in
 File: Dashboard init js
 */


!function($) {
  "use strict";

  var Dashboard = function() {};

  //creates Stacked chart
  Dashboard.prototype.createStackedChart  = function(element, data, xkey, ykeys, labels, lineColors) {
      Morris.Bar({
          element: element,
          data: data,
          xkey: xkey,
          ykeys: ykeys,
          stacked: true,
          labels: labels,
          hideHover: 'auto',
          barSizeRatio: 0.4,
          resize: true, //defaulted to true
          gridLineColor: '#eeeeee',
          barColors: lineColors
      });
  },


  //creates Donut chart
  Dashboard.prototype.createDonutChart = function(element, data, colors) {
      Morris.Donut({
          element: element,
          data: data,
          resize: true, //defaulted to true
          colors: colors
      });
  },

  Dashboard.prototype.init = function() {

      //creating Stacked chart
      var $stckedData  = [
          { y: '2006', a: 180, b: 45, c: 70 },
          { y: '2007', a: 220,  b: 55, c: 65 },
          { y: '2008', a: 230, b: 60, c: 65 },
          { y: '2009', a: 160,  b: 45, c: 60 },
          { y: '2010', a: 210, b: 40, c: 35 },
          { y: '2011', a: 240,  b: 55, c: 60 },
          { y: '2012', a: 190,  b: 40, c: 65 },
          { y: '2013', a: 160,  b: 45, c: 50 },
          { y: '2014', a: 250,  b: 40, c: 60 },
          { y: '2015', a: 150,  b: 60, c: 70 },
          { y: '2016', a: 180, b: 40, c: 45 },
          { y: '2019', a: 250,  b: 40, c: 60 },
          { y: '2018', a: 150,  b: 60, c: 70 },
          { y: '2019', a: 180, b: 40, c: 45 },
      ];
      this.createStackedChart('morris-bar-stacked', $stckedData, 'y', ['a', 'b' ,'c'], ['Series A', 'Series B', 'Series C'], ['#23cbe0','#0e86e7', '#745af1']);

      //creating donut chart
      var $donutData = [

        {label: "Download Sales", value: 20},
        {label: "In-Store Sales", value: 50},
        {label: "Mail-Order Sales", value: 30}
          ];
      this.createDonutChart('morris-donut-example', $donutData, ['#0e86e7','#23cbe0', '#745af1']);
  },
  //init
  $.Dashboard = new Dashboard, $.Dashboard.Constructor = Dashboard
}(window.jQuery),

//initializing
function($) {
  "use strict";
  $.Dashboard.init();
}(window.jQuery);