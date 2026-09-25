# Remote TPC-DS Matrix

Updated: 2026-09-25T15:41:40.234Z.

160/240 blocks; 800/1,200 measured executions.

tpcds/sf10: 600/600 measured executions; 60/60 complete cases.
tpcds/sf100: 200/600 measured executions; 0/60 complete cases.

Numbers below are arithmetic means in milliseconds. A partial entry shows
its sample count; only entries with ten measurements are complete.

Each nonempty cell links to a median-latency executed plan with metrics.

## tpcds/sf10

| Query | Control-off | Control-on | A-off | A-on | B-off | B-on |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q21 | [930.1][p1] | [887.5][p2] | [280.4][p3] | [411.0][p4] | [458.8][p5] | [545.1][p6] |
| Q37 | [945.1][p7] | [1,720.7][p8] | [573.3][p9] | [717.5][p10] | [589.0][p11] | [928.6][p12] |
| Q80 | [1,300.2][p13] | [1,307.4][p14] | [1,362.5][p15] | [1,377.4][p16] | [1,608.8][p17] | [1,561.4][p18] |
| Q39 | [1,840.3][p19] | [1,895.8][p20] | [777.0][p21] | [1,245.0][p22] | [837.7][p23] | [1,294.5][p24] |
| Q27 | [1,677.5][p25] | [2,179.6][p26] | [1,728.0][p27] | [2,393.2][p28] | [2,017.1][p29] | [2,401.9][p30] |
| Q25 | [1,586.5][p31] | [1,520.1][p32] | [1,449.2][p33] | [1,722.1][p34] | [1,897.4][p35] | [2,161.4][p36] |
| Q82 | [1,060.7][p37] | [1,675.3][p38] | [521.3][p39] | [867.2][p40] | [685.1][p41] | [1,106.4][p42] |
| Q17 | [1,095.8][p43] | [1,157.6][p44] | [1,256.4][p45] | [1,486.8][p46] | [1,281.1][p47] | [1,623.3][p48] |
| Q26 | [1,054.4][p49] | [1,931.6][p50] | [823.1][p51] | [1,855.9][p52] | [801.0][p53] | [1,278.5][p54] |
| Q98 | [739.1][p55] | [803.0][p56] | [824.0][p57] | [1,476.7][p58] | [757.9][p59] | [978.0][p60] |

## tpcds/sf100

| Query | Control-off | Control-on | A-off | A-on | B-off | B-on |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q21 | [1,066.1 (n=5)][p61] | [1,162.0 (n=5)][p62] | [335.8 (n=5)][p63] | [496.0 (n=5)][p64] | - | - |
| Q37 | [1,262.0 (n=5)][p65] | [1,707.5 (n=5)][p66] | [719.5 (n=5)][p67] | [913.0 (n=5)][p68] | - | - |
| Q80 | [4,320.5 (n=5)][p69] | [4,411.9 (n=5)][p70] | [3,677.3 (n=5)][p71] | [3,516.8 (n=5)][p72] | - | - |
| Q39 | [3,489.1 (n=5)][p73] | [3,728.4 (n=5)][p74] | [1,586.8 (n=5)][p75] | [1,579.8 (n=5)][p76] | - | - |
| Q27 | [5,721.9 (n=5)][p77] | [6,243.7 (n=5)][p78] | [5,724.1 (n=5)][p79] | [4,510.9 (n=5)][p80] | - | - |
| Q25 | [3,995.6 (n=5)][p81] | [4,303.8 (n=5)][p82] | [4,252.8 (n=5)][p83] | [4,451.6 (n=5)][p84] | - | - |
| Q82 | [2,354.2 (n=5)][p85] | [3,986.5 (n=5)][p86] | [1,221.7 (n=5)][p87] | [1,849.7 (n=5)][p88] | - | - |
| Q17 | [3,329.8 (n=5)][p89] | [3,484.8 (n=5)][p90] | [3,046.4 (n=5)][p91] | [4,991.3 (n=5)][p92] | - | - |
| Q26 | [2,064.9 (n=5)][p93] | [2,377.9 (n=5)][p94] | [1,467.2 (n=5)][p95] | [3,472.7 (n=5)][p96] | - | - |
| Q98 | [1,570.7 (n=5)][p97] | [1,326.6 (n=5)][p98] | [1,712.1 (n=5)][p99] | [2,948.4 (n=5)][p100] | - | - |

## Spread And Samples

| Dataset | Query | Case | n | Mean ms | Median ms | SD ms | Min ms | Max ms |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tpcds/sf10 | q21 | Control-off | 10 | 930.1 | 943.0 | 98.5 | 800.1 | 1,110.7 |
| tpcds/sf10 | q21 | A-off | 10 | 280.4 | 256.8 | 73.8 | 189.1 | 445.4 |
| tpcds/sf10 | q21 | Control-on | 10 | 887.5 | 883.1 | 44.8 | 833.2 | 968.0 |
| tpcds/sf10 | q21 | A-on | 10 | 411.0 | 384.5 | 55.4 | 357.6 | 516.7 |
| tpcds/sf10 | q37 | A-off | 10 | 573.3 | 569.1 | 109.7 | 450.6 | 831.7 |
| tpcds/sf10 | q37 | Control-on | 10 | 1,720.7 | 1,712.1 | 159.7 | 1,542.9 | 2,012.3 |
| tpcds/sf10 | q37 | A-on | 10 | 717.5 | 706.0 | 62.8 | 635.9 | 852.6 |
| tpcds/sf10 | q37 | Control-off | 10 | 945.1 | 910.9 | 112.2 | 819.5 | 1,166.8 |
| tpcds/sf10 | q80 | Control-on | 10 | 1,307.4 | 1,275.9 | 122.0 | 1,151.1 | 1,552.9 |
| tpcds/sf10 | q80 | A-on | 10 | 1,377.4 | 1,371.8 | 101.1 | 1,237.7 | 1,518.6 |
| tpcds/sf10 | q80 | Control-off | 10 | 1,300.2 | 1,339.1 | 84.3 | 1,148.4 | 1,406.8 |
| tpcds/sf10 | q80 | A-off | 10 | 1,362.5 | 1,330.5 | 121.8 | 1,236.7 | 1,630.9 |
| tpcds/sf10 | q39 | A-on | 10 | 1,245.0 | 1,234.3 | 146.6 | 1,041.6 | 1,507.6 |
| tpcds/sf10 | q39 | Control-off | 10 | 1,840.3 | 1,787.7 | 146.2 | 1,694.0 | 2,117.2 |
| tpcds/sf10 | q39 | A-off | 10 | 777.0 | 790.2 | 75.9 | 648.1 | 898.7 |
| tpcds/sf10 | q39 | Control-on | 10 | 1,895.8 | 1,870.8 | 128.2 | 1,740.4 | 2,109.0 |
| tpcds/sf10 | q27 | Control-off | 10 | 1,677.5 | 1,670.6 | 121.2 | 1,387.4 | 1,810.3 |
| tpcds/sf10 | q27 | A-off | 10 | 1,728.0 | 1,741.2 | 126.0 | 1,528.1 | 1,913.5 |
| tpcds/sf10 | q27 | Control-on | 10 | 2,179.6 | 2,123.1 | 330.2 | 1,809.7 | 2,875.0 |
| tpcds/sf10 | q27 | A-on | 10 | 2,393.2 | 2,417.6 | 198.7 | 2,073.9 | 2,747.5 |
| tpcds/sf10 | q25 | A-off | 10 | 1,449.2 | 1,433.6 | 134.4 | 1,247.9 | 1,659.9 |
| tpcds/sf10 | q25 | Control-on | 10 | 1,520.1 | 1,557.2 | 112.8 | 1,327.9 | 1,683.3 |
| tpcds/sf10 | q25 | A-on | 10 | 1,722.1 | 1,701.6 | 190.7 | 1,477.7 | 2,132.6 |
| tpcds/sf10 | q25 | Control-off | 10 | 1,586.5 | 1,476.2 | 284.7 | 1,335.0 | 2,182.4 |
| tpcds/sf10 | q82 | Control-on | 10 | 1,675.3 | 1,651.2 | 111.6 | 1,535.1 | 1,861.0 |
| tpcds/sf10 | q82 | A-on | 10 | 867.2 | 857.9 | 130.4 | 682.7 | 1,070.2 |
| tpcds/sf10 | q82 | Control-off | 10 | 1,060.7 | 989.0 | 177.3 | 864.3 | 1,407.8 |
| tpcds/sf10 | q82 | A-off | 10 | 521.3 | 507.3 | 59.2 | 438.8 | 656.8 |
| tpcds/sf10 | q17 | A-on | 10 | 1,486.8 | 1,511.4 | 108.3 | 1,329.9 | 1,647.7 |
| tpcds/sf10 | q17 | Control-off | 10 | 1,095.8 | 1,077.3 | 89.0 | 993.8 | 1,262.4 |
| tpcds/sf10 | q17 | A-off | 10 | 1,256.4 | 1,225.0 | 132.5 | 1,086.3 | 1,519.6 |
| tpcds/sf10 | q17 | Control-on | 10 | 1,157.6 | 1,159.0 | 91.0 | 1,016.9 | 1,349.7 |
| tpcds/sf10 | q26 | Control-off | 10 | 1,054.4 | 1,058.9 | 109.3 | 891.7 | 1,277.4 |
| tpcds/sf10 | q26 | A-off | 10 | 823.1 | 825.1 | 134.1 | 646.8 | 1,080.4 |
| tpcds/sf10 | q26 | Control-on | 10 | 1,931.6 | 1,960.7 | 402.5 | 1,419.4 | 2,502.8 |
| tpcds/sf10 | q26 | A-on | 10 | 1,855.9 | 1,790.9 | 505.0 | 1,203.6 | 2,661.0 |
| tpcds/sf10 | q98 | A-off | 10 | 824.0 | 788.2 | 274.8 | 536.0 | 1,295.3 |
| tpcds/sf10 | q98 | Control-on | 10 | 803.0 | 681.2 | 279.3 | 534.4 | 1,329.4 |
| tpcds/sf10 | q98 | A-on | 10 | 1,476.7 | 876.2 | 1,422.2 | 605.1 | 4,284.9 |
| tpcds/sf10 | q98 | Control-off | 10 | 739.1 | 752.4 | 198.3 | 506.2 | 1,057.0 |
| tpcds/sf100 | q21 | Control-off | 5 | 1,066.1 | 1,087.5 | 75.1 | 957.7 | 1,146.9 |
| tpcds/sf100 | q21 | A-off | 5 | 335.8 | 338.8 | 13.8 | 319.9 | 350.1 |
| tpcds/sf100 | q21 | Control-on | 5 | 1,162.0 | 1,197.8 | 142.3 | 984.8 | 1,354.9 |
| tpcds/sf100 | q21 | A-on | 5 | 496.0 | 503.2 | 27.0 | 465.7 | 527.3 |
| tpcds/sf100 | q37 | A-off | 5 | 719.5 | 723.8 | 71.0 | 640.3 | 819.9 |
| tpcds/sf100 | q37 | Control-on | 5 | 1,707.5 | 1,725.9 | 104.8 | 1,545.3 | 1,817.7 |
| tpcds/sf100 | q37 | A-on | 5 | 913.0 | 903.5 | 21.0 | 896.1 | 946.2 |
| tpcds/sf100 | q37 | Control-off | 5 | 1,262.0 | 1,317.0 | 140.7 | 1,057.0 | 1,420.3 |
| tpcds/sf100 | q80 | Control-on | 5 | 4,411.9 | 4,351.2 | 276.1 | 4,187.3 | 4,890.2 |
| tpcds/sf100 | q80 | A-on | 5 | 3,516.8 | 3,533.6 | 336.9 | 3,130.8 | 4,036.7 |
| tpcds/sf100 | q80 | Control-off | 5 | 4,320.5 | 4,149.7 | 253.2 | 4,126.6 | 4,639.5 |
| tpcds/sf100 | q80 | A-off | 5 | 3,677.3 | 3,587.5 | 177.3 | 3,510.9 | 3,938.6 |
| tpcds/sf100 | q39 | A-on | 5 | 1,579.8 | 1,497.7 | 315.2 | 1,297.3 | 2,108.9 |
| tpcds/sf100 | q39 | Control-off | 5 | 3,489.1 | 3,453.9 | 371.4 | 3,116.3 | 4,031.1 |
| tpcds/sf100 | q39 | A-off | 5 | 1,586.8 | 1,557.1 | 373.1 | 1,187.5 | 2,196.5 |
| tpcds/sf100 | q39 | Control-on | 5 | 3,728.4 | 3,748.3 | 230.9 | 3,464.7 | 4,060.3 |
| tpcds/sf100 | q27 | Control-off | 5 | 5,721.9 | 5,748.4 | 126.2 | 5,574.7 | 5,893.0 |
| tpcds/sf100 | q27 | A-off | 5 | 5,724.1 | 5,742.1 | 155.5 | 5,521.9 | 5,889.2 |
| tpcds/sf100 | q27 | Control-on | 5 | 6,243.7 | 6,227.3 | 278.3 | 5,934.6 | 6,691.6 |
| tpcds/sf100 | q27 | A-on | 5 | 4,510.9 | 4,470.1 | 324.0 | 4,038.4 | 4,913.0 |
| tpcds/sf100 | q25 | A-off | 5 | 4,252.8 | 4,145.5 | 417.0 | 3,814.7 | 4,935.5 |
| tpcds/sf100 | q25 | Control-on | 5 | 4,303.8 | 4,367.8 | 250.5 | 3,933.3 | 4,576.2 |
| tpcds/sf100 | q25 | A-on | 5 | 4,451.6 | 4,353.1 | 318.5 | 4,092.4 | 4,878.1 |
| tpcds/sf100 | q25 | Control-off | 5 | 3,995.6 | 4,011.7 | 310.5 | 3,624.9 | 4,451.8 |
| tpcds/sf100 | q82 | Control-on | 5 | 3,986.5 | 3,890.2 | 468.3 | 3,491.4 | 4,704.6 |
| tpcds/sf100 | q82 | A-on | 5 | 1,849.7 | 1,831.8 | 204.3 | 1,648.4 | 2,179.9 |
| tpcds/sf100 | q82 | Control-off | 5 | 2,354.2 | 2,287.7 | 167.8 | 2,227.9 | 2,644.7 |
| tpcds/sf100 | q82 | A-off | 5 | 1,221.7 | 1,180.3 | 185.4 | 1,034.6 | 1,501.5 |
| tpcds/sf100 | q17 | A-on | 5 | 4,991.3 | 5,061.2 | 484.3 | 4,532.0 | 5,706.1 |
| tpcds/sf100 | q17 | Control-off | 5 | 3,329.8 | 3,485.4 | 364.7 | 2,814.0 | 3,727.8 |
| tpcds/sf100 | q17 | A-off | 5 | 3,046.4 | 3,081.4 | 93.3 | 2,906.1 | 3,133.5 |
| tpcds/sf100 | q17 | Control-on | 5 | 3,484.8 | 3,677.6 | 287.4 | 3,133.0 | 3,712.0 |
| tpcds/sf100 | q26 | Control-off | 5 | 2,064.9 | 2,037.2 | 176.8 | 1,843.5 | 2,273.4 |
| tpcds/sf100 | q26 | A-off | 5 | 1,467.2 | 1,438.4 | 105.3 | 1,324.6 | 1,599.4 |
| tpcds/sf100 | q26 | Control-on | 5 | 2,377.9 | 2,338.1 | 172.5 | 2,179.9 | 2,628.5 |
| tpcds/sf100 | q26 | A-on | 5 | 3,472.7 | 3,493.6 | 220.7 | 3,232.0 | 3,788.5 |
| tpcds/sf100 | q98 | A-off | 5 | 1,712.1 | 1,752.5 | 157.9 | 1,441.6 | 1,826.1 |
| tpcds/sf100 | q98 | Control-on | 5 | 1,326.6 | 1,333.6 | 75.8 | 1,230.3 | 1,417.5 |
| tpcds/sf100 | q98 | A-on | 5 | 2,948.4 | 3,022.5 | 469.0 | 2,326.0 | 3,474.3 |
| tpcds/sf100 | q98 | Control-off | 5 | 1,570.7 | 1,514.5 | 164.5 | 1,431.7 | 1,822.0 |
| tpcds/sf10 | q21 | B-off | 10 | 458.8 | 359.4 | 194.7 | 291.0 | 899.8 |
| tpcds/sf10 | q21 | B-on | 10 | 545.1 | 525.5 | 144.8 | 370.4 | 765.9 |
| tpcds/sf10 | q37 | B-on | 10 | 928.6 | 905.0 | 68.4 | 837.0 | 1,025.4 |
| tpcds/sf10 | q37 | B-off | 10 | 589.0 | 547.8 | 78.1 | 517.7 | 718.9 |
| tpcds/sf10 | q80 | B-off | 10 | 1,608.8 | 1,542.5 | 257.9 | 1,329.8 | 2,288.0 |
| tpcds/sf10 | q80 | B-on | 10 | 1,561.4 | 1,564.4 | 129.4 | 1,369.4 | 1,751.5 |
| tpcds/sf10 | q39 | B-on | 10 | 1,294.5 | 1,250.2 | 128.4 | 1,145.1 | 1,478.6 |
| tpcds/sf10 | q39 | B-off | 10 | 837.7 | 866.3 | 56.6 | 719.5 | 890.3 |
| tpcds/sf10 | q27 | B-off | 10 | 2,017.1 | 1,903.5 | 415.6 | 1,438.8 | 2,898.7 |
| tpcds/sf10 | q27 | B-on | 10 | 2,401.9 | 2,381.3 | 374.4 | 1,851.3 | 3,044.9 |
| tpcds/sf10 | q25 | B-on | 10 | 2,161.4 | 2,080.9 | 427.9 | 1,725.2 | 2,992.6 |
| tpcds/sf10 | q25 | B-off | 10 | 1,897.4 | 1,873.2 | 155.2 | 1,694.2 | 2,188.0 |
| tpcds/sf10 | q82 | B-off | 10 | 685.1 | 580.1 | 230.1 | 462.5 | 1,188.8 |
| tpcds/sf10 | q82 | B-on | 10 | 1,106.4 | 1,096.0 | 100.1 | 983.1 | 1,319.4 |
| tpcds/sf10 | q17 | B-on | 10 | 1,623.3 | 1,510.0 | 273.2 | 1,363.2 | 2,265.6 |
| tpcds/sf10 | q17 | B-off | 10 | 1,281.1 | 1,296.3 | 112.3 | 1,083.8 | 1,491.4 |
| tpcds/sf10 | q26 | B-off | 10 | 801.0 | 806.1 | 109.6 | 654.2 | 972.4 |
| tpcds/sf10 | q26 | B-on | 10 | 1,278.5 | 1,256.3 | 178.4 | 1,064.5 | 1,679.0 |
| tpcds/sf10 | q98 | B-on | 10 | 978.0 | 961.5 | 194.6 | 641.9 | 1,250.8 |
| tpcds/sf10 | q98 | B-off | 10 | 757.9 | 658.9 | 232.0 | 555.2 | 1,298.3 |

## Independent Passes

| Dataset | Query | Case | First block ms | Second block ms | Mean tasks |
| --- | --- | --- | ---: | ---: | ---: |
| tpcds/sf10 | q21 | Control-off | 975.9 | 884.4 | 7.0 |
| tpcds/sf10 | q21 | A-off | 259.4 | 301.4 | 7.0 |
| tpcds/sf10 | q21 | Control-on | 860.9 | 914.2 | 7.0 |
| tpcds/sf10 | q21 | A-on | 436.6 | 385.5 | 7.0 |
| tpcds/sf10 | q37 | A-off | 602.5 | 544.1 | 40.0 |
| tpcds/sf10 | q37 | Control-on | 1,688.7 | 1,752.7 | 40.0 |
| tpcds/sf10 | q37 | A-on | 710.1 | 724.8 | 40.0 |
| tpcds/sf10 | q37 | Control-off | 915.5 | 974.7 | 40.0 |
| tpcds/sf10 | q80 | Control-on | 1,293.2 | 1,321.7 | 106.0 |
| tpcds/sf10 | q80 | A-on | 1,382.2 | 1,372.6 | 106.0 |
| tpcds/sf10 | q80 | Control-off | 1,299.9 | 1,300.5 | 106.0 |
| tpcds/sf10 | q80 | A-off | 1,307.7 | 1,417.3 | 106.0 |
| tpcds/sf10 | q39 | A-on | 1,208.1 | 1,282.0 | 16.0 |
| tpcds/sf10 | q39 | Control-off | 1,749.4 | 1,931.1 | 16.0 |
| tpcds/sf10 | q39 | A-off | 743.3 | 810.6 | 16.0 |
| tpcds/sf10 | q39 | Control-on | 1,887.3 | 1,904.3 | 16.0 |
| tpcds/sf10 | q27 | Control-off | 1,667.0 | 1,688.0 | 96.0 |
| tpcds/sf10 | q27 | A-off | 1,810.2 | 1,645.7 | 96.0 |
| tpcds/sf10 | q27 | Control-on | 2,429.2 | 1,930.1 | 96.0 |
| tpcds/sf10 | q27 | A-on | 2,519.7 | 2,266.6 | 96.0 |
| tpcds/sf10 | q25 | A-off | 1,476.6 | 1,421.8 | 68.0 |
| tpcds/sf10 | q25 | Control-on | 1,482.4 | 1,557.7 | 68.0 |
| tpcds/sf10 | q25 | A-on | 1,614.9 | 1,829.4 | 68.0 |
| tpcds/sf10 | q25 | Control-off | 1,429.8 | 1,743.3 | 68.0 |
| tpcds/sf10 | q82 | Control-on | 1,638.9 | 1,711.8 | 40.0 |
| tpcds/sf10 | q82 | A-on | 856.3 | 878.2 | 40.0 |
| tpcds/sf10 | q82 | Control-off | 982.1 | 1,139.2 | 40.0 |
| tpcds/sf10 | q82 | A-off | 493.8 | 548.8 | 40.0 |
| tpcds/sf10 | q17 | A-on | 1,471.5 | 1,502.1 | 68.0 |
| tpcds/sf10 | q17 | Control-off | 1,148.0 | 1,043.6 | 68.0 |
| tpcds/sf10 | q17 | A-off | 1,259.4 | 1,253.4 | 68.0 |
| tpcds/sf10 | q17 | Control-on | 1,172.2 | 1,143.0 | 68.0 |
| tpcds/sf10 | q26 | Control-off | 1,081.5 | 1,027.3 | 40.0 |
| tpcds/sf10 | q26 | A-off | 791.4 | 854.7 | 40.0 |
| tpcds/sf10 | q26 | Control-on | 1,644.4 | 2,218.8 | 40.0 |
| tpcds/sf10 | q26 | A-on | 1,468.6 | 2,243.2 | 40.0 |
| tpcds/sf10 | q98 | A-off | 581.6 | 1,066.3 | 38.0 |
| tpcds/sf10 | q98 | Control-on | 573.1 | 1,032.8 | 38.0 |
| tpcds/sf10 | q98 | A-on | 717.1 | 2,236.2 | 38.0 |
| tpcds/sf10 | q98 | Control-off | 575.4 | 902.9 | 38.0 |
| tpcds/sf100 | q21 | Control-off | 1,066.1 | - | 15.0 |
| tpcds/sf100 | q21 | A-off | 335.8 | - | 15.0 |
| tpcds/sf100 | q21 | Control-on | 1,162.0 | - | 15.0 |
| tpcds/sf100 | q21 | A-on | 496.0 | - | 15.0 |
| tpcds/sf100 | q37 | A-off | 719.5 | - | 44.0 |
| tpcds/sf100 | q37 | Control-on | 1,707.5 | - | 44.0 |
| tpcds/sf100 | q37 | A-on | 913.0 | - | 44.0 |
| tpcds/sf100 | q37 | Control-off | 1,262.0 | - | 44.0 |
| tpcds/sf100 | q80 | Control-on | 4,411.9 | - | 139.0 |
| tpcds/sf100 | q80 | A-on | 3,516.8 | - | 139.0 |
| tpcds/sf100 | q80 | Control-off | 4,320.5 | - | 139.0 |
| tpcds/sf100 | q80 | A-off | 3,677.3 | - | 139.0 |
| tpcds/sf100 | q39 | A-on | 1,579.8 | - | 36.0 |
| tpcds/sf100 | q39 | Control-off | 3,489.1 | - | 36.0 |
| tpcds/sf100 | q39 | A-off | 1,586.8 | - | 36.0 |
| tpcds/sf100 | q39 | Control-on | 3,728.4 | - | 36.0 |
| tpcds/sf100 | q27 | Control-off | 5,721.9 | - | 132.0 |
| tpcds/sf100 | q27 | A-off | 5,724.1 | - | 132.0 |
| tpcds/sf100 | q27 | Control-on | 6,243.7 | - | 132.0 |
| tpcds/sf100 | q27 | A-on | 4,510.9 | - | 132.0 |
| tpcds/sf100 | q25 | A-off | 4,252.8 | - | 89.0 |
| tpcds/sf100 | q25 | Control-on | 4,303.8 | - | 89.0 |
| tpcds/sf100 | q25 | A-on | 4,451.6 | - | 89.0 |
| tpcds/sf100 | q25 | Control-off | 3,995.6 | - | 89.0 |
| tpcds/sf100 | q82 | Control-on | 3,986.5 | - | 44.0 |
| tpcds/sf100 | q82 | A-on | 1,849.7 | - | 44.0 |
| tpcds/sf100 | q82 | Control-off | 2,354.2 | - | 44.0 |
| tpcds/sf100 | q82 | A-off | 1,221.7 | - | 44.0 |
| tpcds/sf100 | q17 | A-on | 4,991.3 | - | 89.0 |
| tpcds/sf100 | q17 | Control-off | 3,329.8 | - | 89.0 |
| tpcds/sf100 | q17 | A-off | 3,046.4 | - | 89.0 |
| tpcds/sf100 | q17 | Control-on | 3,484.8 | - | 89.0 |
| tpcds/sf100 | q26 | Control-off | 2,064.9 | - | 52.0 |
| tpcds/sf100 | q26 | A-off | 1,467.2 | - | 52.0 |
| tpcds/sf100 | q26 | Control-on | 2,377.9 | - | 52.0 |
| tpcds/sf100 | q26 | A-on | 3,472.7 | - | 52.0 |
| tpcds/sf100 | q98 | A-off | 1,712.1 | - | 38.0 |
| tpcds/sf100 | q98 | Control-on | 1,326.6 | - | 38.0 |
| tpcds/sf100 | q98 | A-on | 2,948.4 | - | 38.0 |
| tpcds/sf100 | q98 | Control-off | 1,570.7 | - | 38.0 |
| tpcds/sf10 | q21 | B-off | 596.6 | 320.9 | 7.0 |
| tpcds/sf10 | q21 | B-on | 603.9 | 486.3 | 7.0 |
| tpcds/sf10 | q37 | B-on | 930.9 | 926.4 | 40.0 |
| tpcds/sf10 | q37 | B-off | 601.6 | 576.4 | 40.0 |
| tpcds/sf10 | q80 | B-off | 1,710.5 | 1,507.1 | 106.0 |
| tpcds/sf10 | q80 | B-on | 1,494.2 | 1,628.6 | 106.0 |
| tpcds/sf10 | q39 | B-on | 1,247.3 | 1,341.8 | 16.0 |
| tpcds/sf10 | q39 | B-off | 822.7 | 852.7 | 16.0 |
| tpcds/sf10 | q27 | B-off | 2,160.8 | 1,873.4 | 96.0 |
| tpcds/sf10 | q27 | B-on | 2,551.3 | 2,252.5 | 96.0 |
| tpcds/sf10 | q25 | B-on | 1,855.8 | 2,467.0 | 68.0 |
| tpcds/sf10 | q25 | B-off | 1,943.9 | 1,851.0 | 68.0 |
| tpcds/sf10 | q82 | B-off | 545.8 | 824.4 | 40.0 |
| tpcds/sf10 | q82 | B-on | 1,053.9 | 1,158.8 | 40.0 |
| tpcds/sf10 | q17 | B-on | 1,603.7 | 1,642.8 | 68.0 |
| tpcds/sf10 | q17 | B-off | 1,313.4 | 1,248.8 | 68.0 |
| tpcds/sf10 | q26 | B-off | 788.8 | 813.2 | 40.0 |
| tpcds/sf10 | q26 | B-on | 1,149.2 | 1,407.7 | 40.0 |
| tpcds/sf10 | q98 | B-on | 933.8 | 1,022.3 | 38.0 |
| tpcds/sf10 | q98 | B-off | 876.4 | 639.4 | 38.0 |

[p1]: plans/tpcds-sf10-q21-Control-off.txt
[p2]: plans/tpcds-sf10-q21-Control-on.txt
[p3]: plans/tpcds-sf10-q21-A-off.txt
[p4]: plans/tpcds-sf10-q21-A-on.txt
[p5]: plans/tpcds-sf10-q21-B-off.txt
[p6]: plans/tpcds-sf10-q21-B-on.txt
[p7]: plans/tpcds-sf10-q37-Control-off.txt
[p8]: plans/tpcds-sf10-q37-Control-on.txt
[p9]: plans/tpcds-sf10-q37-A-off.txt
[p10]: plans/tpcds-sf10-q37-A-on.txt
[p11]: plans/tpcds-sf10-q37-B-off.txt
[p12]: plans/tpcds-sf10-q37-B-on.txt
[p13]: plans/tpcds-sf10-q80-Control-off.txt
[p14]: plans/tpcds-sf10-q80-Control-on.txt
[p15]: plans/tpcds-sf10-q80-A-off.txt
[p16]: plans/tpcds-sf10-q80-A-on.txt
[p17]: plans/tpcds-sf10-q80-B-off.txt
[p18]: plans/tpcds-sf10-q80-B-on.txt
[p19]: plans/tpcds-sf10-q39-Control-off.txt
[p20]: plans/tpcds-sf10-q39-Control-on.txt
[p21]: plans/tpcds-sf10-q39-A-off.txt
[p22]: plans/tpcds-sf10-q39-A-on.txt
[p23]: plans/tpcds-sf10-q39-B-off.txt
[p24]: plans/tpcds-sf10-q39-B-on.txt
[p25]: plans/tpcds-sf10-q27-Control-off.txt
[p26]: plans/tpcds-sf10-q27-Control-on.txt
[p27]: plans/tpcds-sf10-q27-A-off.txt
[p28]: plans/tpcds-sf10-q27-A-on.txt
[p29]: plans/tpcds-sf10-q27-B-off.txt
[p30]: plans/tpcds-sf10-q27-B-on.txt
[p31]: plans/tpcds-sf10-q25-Control-off.txt
[p32]: plans/tpcds-sf10-q25-Control-on.txt
[p33]: plans/tpcds-sf10-q25-A-off.txt
[p34]: plans/tpcds-sf10-q25-A-on.txt
[p35]: plans/tpcds-sf10-q25-B-off.txt
[p36]: plans/tpcds-sf10-q25-B-on.txt
[p37]: plans/tpcds-sf10-q82-Control-off.txt
[p38]: plans/tpcds-sf10-q82-Control-on.txt
[p39]: plans/tpcds-sf10-q82-A-off.txt
[p40]: plans/tpcds-sf10-q82-A-on.txt
[p41]: plans/tpcds-sf10-q82-B-off.txt
[p42]: plans/tpcds-sf10-q82-B-on.txt
[p43]: plans/tpcds-sf10-q17-Control-off.txt
[p44]: plans/tpcds-sf10-q17-Control-on.txt
[p45]: plans/tpcds-sf10-q17-A-off.txt
[p46]: plans/tpcds-sf10-q17-A-on.txt
[p47]: plans/tpcds-sf10-q17-B-off.txt
[p48]: plans/tpcds-sf10-q17-B-on.txt
[p49]: plans/tpcds-sf10-q26-Control-off.txt
[p50]: plans/tpcds-sf10-q26-Control-on.txt
[p51]: plans/tpcds-sf10-q26-A-off.txt
[p52]: plans/tpcds-sf10-q26-A-on.txt
[p53]: plans/tpcds-sf10-q26-B-off.txt
[p54]: plans/tpcds-sf10-q26-B-on.txt
[p55]: plans/tpcds-sf10-q98-Control-off.txt
[p56]: plans/tpcds-sf10-q98-Control-on.txt
[p57]: plans/tpcds-sf10-q98-A-off.txt
[p58]: plans/tpcds-sf10-q98-A-on.txt
[p59]: plans/tpcds-sf10-q98-B-off.txt
[p60]: plans/tpcds-sf10-q98-B-on.txt
[p61]: plans/tpcds-sf100-q21-Control-off.txt
[p62]: plans/tpcds-sf100-q21-Control-on.txt
[p63]: plans/tpcds-sf100-q21-A-off.txt
[p64]: plans/tpcds-sf100-q21-A-on.txt
[p65]: plans/tpcds-sf100-q37-Control-off.txt
[p66]: plans/tpcds-sf100-q37-Control-on.txt
[p67]: plans/tpcds-sf100-q37-A-off.txt
[p68]: plans/tpcds-sf100-q37-A-on.txt
[p69]: plans/tpcds-sf100-q80-Control-off.txt
[p70]: plans/tpcds-sf100-q80-Control-on.txt
[p71]: plans/tpcds-sf100-q80-A-off.txt
[p72]: plans/tpcds-sf100-q80-A-on.txt
[p73]: plans/tpcds-sf100-q39-Control-off.txt
[p74]: plans/tpcds-sf100-q39-Control-on.txt
[p75]: plans/tpcds-sf100-q39-A-off.txt
[p76]: plans/tpcds-sf100-q39-A-on.txt
[p77]: plans/tpcds-sf100-q27-Control-off.txt
[p78]: plans/tpcds-sf100-q27-Control-on.txt
[p79]: plans/tpcds-sf100-q27-A-off.txt
[p80]: plans/tpcds-sf100-q27-A-on.txt
[p81]: plans/tpcds-sf100-q25-Control-off.txt
[p82]: plans/tpcds-sf100-q25-Control-on.txt
[p83]: plans/tpcds-sf100-q25-A-off.txt
[p84]: plans/tpcds-sf100-q25-A-on.txt
[p85]: plans/tpcds-sf100-q82-Control-off.txt
[p86]: plans/tpcds-sf100-q82-Control-on.txt
[p87]: plans/tpcds-sf100-q82-A-off.txt
[p88]: plans/tpcds-sf100-q82-A-on.txt
[p89]: plans/tpcds-sf100-q17-Control-off.txt
[p90]: plans/tpcds-sf100-q17-Control-on.txt
[p91]: plans/tpcds-sf100-q17-A-off.txt
[p92]: plans/tpcds-sf100-q17-A-on.txt
[p93]: plans/tpcds-sf100-q26-Control-off.txt
[p94]: plans/tpcds-sf100-q26-Control-on.txt
[p95]: plans/tpcds-sf100-q26-A-off.txt
[p96]: plans/tpcds-sf100-q26-A-on.txt
[p97]: plans/tpcds-sf100-q98-Control-off.txt
[p98]: plans/tpcds-sf100-q98-Control-on.txt
[p99]: plans/tpcds-sf100-q98-A-off.txt
[p100]: plans/tpcds-sf100-q98-A-on.txt
