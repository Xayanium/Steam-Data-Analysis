/*
 Template Name: Zegva - Responsive Bootstrap 4 Admin Dashboard
 Author: Themesdesign
 Website: www.themesdesign.in
 File: C3 Chart init js
 */

!function($) {
    "use strict";

    var ChartC3 = function() {};

    ChartC3.prototype.init = function () {
        //generating chart 
        c3.generate({
            bindto: '#chart',
            data: {
                columns: [
                    ['Desktop', 180, 100, 200, 200, 250, 180],
                    ['Mobile', 250, 130, 230, 170, 180, 250],
                    ['Tablet', 300, 180, 280, 120, 150, 300]
                ],
                type: 'bar',
                colors: {
                    Desktop: '#745af1',
                    Mobile: '#0e86e7',
                    Tablet: '#23cbe0'
                }
            }
        });

        //combined chart
        c3.generate({
            bindto: '#combine-chart',
            data: {
                columns: [
                    ['SonyVaio', 80, 20, 120, 80, 40, 90],
                    ['iMacs', 200, 130, 90, 200, 130, 220],
                    ['Tablets', 300, 200, 160, 300, 300, 230],
                    ['iPhones', 250, 160, 80, 180, 250, 120],
                    ['Macbooks', 160, 120, 160, 140, 80, 130]
                ],
                types: {
                    SonyVaio: 'bar',
                    iMacs: 'bar',
                    Tablets: 'spline',
                    iPhones: 'line',
                    Macbooks: 'bar'
                },
                colors: {
                    SonyVaio: '#0e86e7',
                    iMacs: '#23cbe0',
                    Tablets: '#fb4365',
                    iPhones: '#fdaf27',
                    Macbooks: '#745af1'
                },
                groups: [
                    ['SonyVaio','iMacs']
                ]
            },
            axis: {
                x: {
                    type: 'categorized'
                }
            }
        });
        
        //roated chart
        c3.generate({
            bindto: '#roated-chart',
            data: {
                columns: [
                ['Revenue', 80, 100, 140, 250, 150, 80],
                ['Pageview', 40, 30, 10, 20, 50, 30]
                ],
                types: {
                    Revenue: 'bar'
                },
                colors: {
                    Revenue: '#0e86e7',
                    Pageview: '#23cbe0'
	            }
            },
            axis: {
                rotated: true,
                x: {
                type: 'categorized'
                }
            }
        });

        //stacked chart
        c3.generate({
            bindto: '#chart-stacked',
            data: {
                columns: [
                    ['Revenue', 130, 120, 150, 120, 160, 150, 250, 120, 180, 140, 160, 150],
                    ['Pageview', 10, 150, 90, 240, 130, 220, 200, 130, 90, 240, 130, 220]
                ],
                types: {
                    Revenue: 'area-spline',
                    Pageview: 'area-spline'
                    // 'line', 'spline', 'step', 'area', 'area-step' are also available to stack
                },
                colors: {
                    Revenue: '#f0f4f7',
                    Pageview: '#23cbe0'
                }
            }
        });
        
        //Donut Chart
        c3.generate({
             bindto: '#donut-chart',
            data: {
                columns: [
                    ['Desktops', 70],
                    ['Smart Phones', 65],
                    ['Mobiles', 70],
                    ['Tablets', 95]
                ],
                type : 'donut'
            },
            donut: {
                title: "Candidates",
                width: 30,
				label: { 
					show:false
				}
            },
            color: {
            	pattern: ['#23cbe0', "#f0f4f7", '#0e86e7', '#745af1']
            }
        });
        
        //Pie Chart
        c3.generate({
             bindto: '#pie-chart',
            data: {
                columns: [
                    ['Desktops', 75],
                    ['Smart Phones', 45],
                    ['Mobiles', 35],
                    ['Tablets', 45]
                ],
                type : 'pie'
            },
            color: {
                pattern: ["#23cbe0", "#f0f4f7","#745af1","#0e86e7"]
            },
            pie: {
		        label: {
		          show: false
		        }
		    }
        });

    },
    $.ChartC3 = new ChartC3, $.ChartC3.Constructor = ChartC3

}(window.jQuery),

//initializing 
function($) {
    "use strict";
    $.ChartC3.init()
}(window.jQuery);



