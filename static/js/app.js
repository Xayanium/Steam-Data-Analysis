/*
 Template Name: Zegva - Responsive Bootstrap 4 Admin Dashboard
 Author: Themesdesign
 Website: www.themesdesign.in
 File: Main js
 */


!function($) {
    "use strict";

    var MainApp = function () {
        this.$body = $("body"),
            this.$wrapper = $("#wrapper"),
            this.$btnFullScreen = $("#btn-fullscreen"),
            this.$leftMenuButton = $('.button-menu-mobile'),
            this.$menuItem = $('.has_sub > a')
    };
    
    MainApp.prototype.initSlimscroll = function () {
        $('.slimscroll').slimscroll({
            height: 'auto',
            position: 'right',
            size: "7px",
            color: '#9ea5ab',
            touchScrollStep: 50
        });
    },

    MainApp.prototype.initNavbar = function () {

        $('.navbar-toggle').on('click', function (event) {
            $(this).toggleClass('open');
            $('#navigation').slideToggle(400);
        });

        $('.navigation-menu>li').slice(-1).addClass('last-elements');

        $('.navigation-menu li.has-submenu a[href="#"]').on('click', function (e) {
            if ($(window).width() < 992) {
                e.preventDefault();
                $(this).parent('li').toggleClass('open').find('.submenu:first').toggleClass('open');
            }
        });
    },

    MainApp.prototype.initActiveMenu = function () {
        $(".navigation-menu a").each(function () {
            var pageUrl = window.location.href.split(/[?#]/)[0];
            if (this.href == pageUrl) { 
                $(this).parent().addClass("active"); // add active to li of the current link
                $(this).parent().parent().parent().addClass("active"); // add active class to an anchor
                $(this).parent().parent().parent().parent().parent().addClass("active"); // add active class to an anchor
            }
        });
    },

    MainApp.prototype.initComponents = function () {
        $('[data-toggle="tooltip"]').tooltip();
        $('[data-toggle="popover"]').popover();
    },
    MainApp.prototype.initToggleSearch = function () {
        $('.toggle-search').on('click', function () {
            var targetId = $(this).data('target');
            var $searchBar;
            if (targetId) {
                $searchBar = $(targetId);
                $searchBar.toggleClass('open');
            }
        });
    },

    //full screen
    MainApp.prototype.initFullScreen = function () {
        var $this = this;
        $this.$btnFullScreen.on("click", function (e) {
            e.preventDefault();

            if (!document.fullscreenElement && /* alternative standard method */ !document.mozFullScreenElement && !document.webkitFullscreenElement) {  // current working methods
                if (document.documentElement.requestFullscreen) {
                    document.documentElement.requestFullscreen();
                } else if (document.documentElement.mozRequestFullScreen) {
                    document.documentElement.mozRequestFullScreen();
                } else if (document.documentElement.webkitRequestFullscreen) {
                    document.documentElement.webkitRequestFullscreen(Element.ALLOW_KEYBOARD_INPUT);
                }
            } else {
                if (document.cancelFullScreen) {
                    document.cancelFullScreen();
                } else if (document.mozCancelFullScreen) {
                    document.mozCancelFullScreen();
                } else if (document.webkitCancelFullScreen) {
                    document.webkitCancelFullScreen();
                }
            }
        });
    },

    MainApp.prototype.initdatepicker= function () {

            //   datepicker
            jQuery('#datepicker').datepicker( {
            todayHighlight: true, viewDate: true
        }
        );
        jQuery('#datepicker').datepicker('setDate', new Date());
    },


    MainApp.prototype.init = function () {
        this.initSlimscroll();
        this.initNavbar();
        this.initActiveMenu();
        this.initComponents();
        this.initFullScreen();
        this.initToggleSearch();
        this.initdatepicker();
        Waves.init();
    },

    //init
    $.MainApp = new MainApp, $.MainApp.Constructor = MainApp
}(window.jQuery),

//initializing
function ($) {
    "use strict";
    $.MainApp.init();
}(window.jQuery);


  //   apex chart
  var options1= {
    chart: {
        type: 'area',
        height: 60,
        sparkline: {
            enabled: true
        }
    }
    ,
    series: [ {
        data: [24, 66, 42, 88, 62, 24, 45, 12, 36, 10]
    }
    ],
    stroke: {
        curve: 'smooth', width: 3
    }
    ,
    colors: ['#0e86e7'],
    tooltip: {
        fixed: {
            enabled: false
        }
        ,
        x: {
            show: false
        }
        ,
        y: {
            title: {
                formatter: function (seriesName) {
                    return ''
                }
            }
        }
        ,
        marker: {
            show: false
        }
    }
}
new ApexCharts(document.querySelector("#chart1"), options1).render();
// 2
var options2= {
    chart: {
        type: 'area',
        height: 60,
        sparkline: {
            enabled: true
        }
    }
    ,
    series: [ {
        data: [54, 12, 24, 66, 42, 25, 44, 12, 36, 9]
    }
    ],
    stroke: {
        curve: 'smooth', width: 3
    }
    ,
    colors: ['#fbb131'],
    tooltip: {
        fixed: {
            enabled: false
        }
        ,
        x: {
            show: false
        }
        ,
        y: {
            title: {
                formatter: function (seriesName) {
                    return ''
                }
            }
        }
        ,
        marker: {
            show: false
        }
    }
}
new ApexCharts(document.querySelector("#chart2"), options2).render();
// 3
var options3= {
    chart: {
        type: 'area',
        height: 60,
        sparkline: {
            enabled: true
        }
    }
    ,
    series: [ {
        data: [10, 36, 12, 44, 63, 24, 44, 12, 56, 24]
    }
    ],
    stroke: {
        curve: 'smooth', width: 3
    }
    ,
    colors: ['#23cbe0'],
    tooltip: {
        fixed: {
            enabled: false
        }
        ,
        x: {
            show: false
        }
        ,
        y: {
            title: {
                formatter: function (seriesName) {
                    return ''
                }
            }
        }
        ,
        marker: {
            show: false
        }
    }
}
new ApexCharts(document.querySelector("#chart3"), options3).render();
//   4
var options4= {
    chart: {
        type: 'area',
        height: 60,
        sparkline: {
            enabled: true
        }
    }
    ,
    series: [ {
        data: [34, 66, 42, 33, 62, 24, 45, 16, 48, 10]
    }
    ],
    stroke: {
        curve: 'smooth', width: 3
    }
    ,
    colors: ['#fb4365'],
    tooltip: {
        fixed: {
            enabled: false
        }
        ,
        x: {
            show: false
        }
        ,
        y: {
            title: {
                formatter: function (seriesName) {
                    return ''
                }
            }
        }
        ,
        marker: {
            show: false
        }
    }
}
new ApexCharts(document.querySelector("#chart4"), options4).render();