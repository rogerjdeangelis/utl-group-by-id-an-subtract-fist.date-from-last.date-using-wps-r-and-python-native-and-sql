%let pgm=utl-group-by-id-an-subtract-fist.date-from-last.date-using-wps-r-and-python-native-and-sql;

Group by id an subtract fist.date from last.date using wps r and python native and sql

github
https://tinyurl.com/2j3m5ymp
https://github.com/rogerjdeangelis/utl-group-by-id-an-subtract-fist.date-from-last.date-using-wps-r-and-python-native-and-sql

StackOverflow R
https://tinyurl.com/3s9rp5jd
https://stackoverflow.com/questions/76771821/mathematical-manipulation-based-on-condition-in-other-columns-r

  SOLUTIONS

      1 wps sqlL
      2 wps datastep
      3 wps r sql
      4 wps python sql
      5 r native
        https://stackoverflow.com/users/12109788/jpsmith


options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
input ID daystoevent excision died;
cards4;
1 19116 0 0
1 19201 1 0
1 19399 0 0
1 19416 0 0
2 17017 1 0
2 17036 0 0
2 17085 0 1
3 17017 1 0
3 17036 1 0
3 17085 0 1
;;;;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  SD1.HAVE                                |  RULES                                      |   OUTPUT                      */
/*                                          |                                             |                               */
/*  ID    DAYSTOEVENT    EXCISION    DIED   |  NEWCOLUMN  = Missing - . bcause no DIED=1  |   The WPS System              */
/*                                          |                                             |                               */
/*   1       19116           0         0    |                                             |   Obs    ID    newcolumn      */
/*   1       19201           1         0    |                                             |                               */
/*   1       19399           0         0    |                                             |    1      1         .         */
/*   1       19416           0         0    |                                             |    2      2        68         */
/*                                          |                                             |    3      3        68         */
/*                                          |  IF FIRST.EXCISION=1 AND LAST.DIED=1        |                               */
/*                                          |                                             |                               */
/*   2       17017           1         0    |  NEWCOLUMN = 68 because 17085 - 17017 =68   |                               */
/*   2       17036           0         0    |                                             |                               */
/*   2       17085           0         1    |                                             |                               */
/*                                          |                                             |                               */
/*   3       17017           1         0    |  HEWCOLUM = 68 because   17085 - 17017 =68  |                               */
/*   3       17036           1         0    |                                             |                               */
/*   3       17085           0         1    |                                             |                               */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                                  _
/ | __      ___ __  ___   ___  __ _| |
| | \ \ /\ / / `_ \/ __| / __|/ _` | |
| |  \ V  V /| |_) \__ \ \__ \ (_| | |
|_|   \_/\_/ | .__/|___/ |___/\__, |_|
             |_|                 |_|
*/

%utl_submit_wps64x('


libname sd1 "d:/sd1";

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;
options validvarname=any;

proc sql;

   create
      table sd1.want as
   select
      id
     ,case
       when (max(daystoevent) - min(daystoevent))  = 0 then .
       else  max(daystoevent) - min(daystoevent)
      end as newcolumn
  from
      sd1.have
  where
      excision=1 or died=1
  group
      by id

;quit;
proc print;
run;quit;

');

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/*  Obs    ID    newcolumn                                                                                                */
/*                                                                                                                        */
/*   1      1         .                                                                                                   */
/*   2      2        68                                                                                                   */
/*   3      3        68                                                                                                   */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                             _       _            _
|___ \  __      ___ __  ___    __| | __ _| |_ __ _ ___| |_ ___ _ __
  __) | \ \ /\ / / `_ \/ __|  / _` |/ _` | __/ _` / __| __/ _ \ `_ \
 / __/   \ V  V /| |_) \__ \ | (_| | (_| | || (_| \__ \ ||  __/ |_) |
|_____|   \_/\_/ | .__/|___/  \__,_|\__,_|\__\__,_|___/\__\___| .__/
                 |_|                                          |_|
*/

%utl_submit_wps64x('
libname sd1 "d:/sd1";
data sd1.want;
  retain first_daystoevent last_daystoevent .;
  do until (last.id);
    set sd1.have;
    by id;
    if  last.id and died    =1 then last_daystoevent=daystoevent;
    if  first.id and excision=1 then  first_daystoevent=daystoevent;
  end;
  newcolumn = last_daystoevent - first_daystoevent;
  keep id newcolumn;
run;quit;
proc print data=sd1.want;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/*  Obs    ID    NEWCOLUMN                                                                                                */
/*                                                                                                                        */
/*   1      1         .                                                                                                   */
/*   2      2        68                                                                                                   */
/*   3      3        68                                                                                                   */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                                         _
|___ /  __      ___ __  ___   _ __   ___  __ _| |
  |_ \  \ \ /\ / / `_ \/ __| | `__| / __|/ _` | |
 ___) |  \ V  V /| |_) \__ \ | |    \__ \ (_| | |
|____/    \_/\_/ | .__/|___/ |_|    |___/\__, |_|
                 |_|                        |_|
*/
proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('
libname sd1 "d:/sd1";
proc r;
export data=sd1.have r=have;
submit;
library(sqldf);
want <- sqldf("
   select
      id
     ,case
       when (max(daystoevent) - min(daystoevent))  = 0 then NULL
       else  max(daystoevent) - min(daystoevent)
      end as newcolumn
  from
      have
  where
      excision=1 or died=1
  group
      by id
");
want;
endsubmit;
proc print;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  R                                                                                                                     */
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/*    ID newcolumn                                                                                                        */
/*  1  1        NA                                                                                                        */
/*  2  2        68                                                                                                        */
/*  3  3        68                                                                                                        */
/*                                                                                                                        */
/*  WPS                                                                                                                   */
/*                                                                                                                        */
/* Obs   ID    NEWCOLUMN                                                                                                  */
/*                                                                                                                        */
/* 1      1         .                                                                                                     */
/* 2      2        68                                                                                                     */
/* 3      3        68                                                                                                     */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*  _                                      _   _                             _
| || |   __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| || |_  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
|__   _|  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
   |_|     \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                  |_|         |_|    |___/                                |_|
*/

%utl_submit_wps64x('

libname sd1 "d:/sd1";
proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

proc python;
export data=sd1.have python=have;
submit;
 from os import path;
 import pandas as pd;
 import numpy as np;
 import pandas as pd;
 from pandasql import sqldf;
 mysql = lambda q: sqldf(q, globals());
 from pandasql import PandaSQL;
 pdsql = PandaSQL(persist=True);
 sqlite3conn = next(pdsql.conn.gen).connection.connection;
 sqlite3conn.enable_load_extension(True);
 sqlite3conn.load_extension("c:/temp/libsqlitefunctions.dll");
 mysql = lambda q: sqldf(q, globals());
 want=pdsql("""
   select
      id
     ,case
       when (max(daystoevent) - min(daystoevent))  = 0 then NULL
       else  max(daystoevent) - min(daystoevent)
      end as newcolumn
  from
      have
  where
      excision=1 or died=1
  group
      by id
""");
print(want);
endsubmit;
import data=sd1.want python=want;
run;quit;
proc print data=sd1.want;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/*  The PYTHON Procedure                                                                                                  */
/*                                                                                                                        */
/*      ID  newcolumn                                                                                                     */
/*  0  1.0        NaN                                                                                                     */
/*  1  2.0       68.0                                                                                                     */
/*  2  3.0       68.0                                                                                                     */
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/*  Obs    ID    NEWCOLUMN                                                                                                */
/*                                                                                                                        */
/*   1      1         .                                                                                                   */
/*   2      2        68                                                                                                   */
/*   3      3        68                                                                                                   */
/*                                                                                                                        */
/**************************************************************************************************************************/


/*___                       _   _
| ___|   _ __   _ __   __ _| |_(_)_   _____
|___ \  | `__| | `_ \ / _` | __| \ \ / / _ \
 ___) | | |    | | | | (_| | |_| |\ V /  __/
|____/  |_|    |_| |_|\__,_|\__|_| \_/ \___|

*/

%utl_submit_wps64x('
libname sd1 "d:/sd1";
proc r;
export data=sd1.have r=have;
submit;
library(tidyverse);
want <- have %>%
  group_by(ID) %>%
  filter(any(DIED == 1)) %>%
  summarise(newcol = DAYSTOEVENT[DIED == 1][1] - DAYSTOEVENT[EXCISION == 1][1]);
endsubmit;
import data=sd1.want python=want;
proc print data=sd1.want;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/* Obs    ID    NEWCOLUMN                                                                                                 */
/*                                                                                                                        */
/*  1      1         .                                                                                                    */
/*  2      2        68                                                                                                    */
/*  3      3        68                                                                                                    */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
