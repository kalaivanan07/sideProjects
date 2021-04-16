ANALYZE TABLE m_date;
ANALYZE TABLE eqty_price_vol;
ANALYZE TABLE m_date_url;
ANALYZE TABLE st_indx_price_vol;
ANALYZE TABLE st_indx_price_vol_2;
ANALYZE TABLE indx_price_vol;
ANALYZE TABLE st_eqty_dlvry_pos;
ANALYZE TABLE eqty_dlvry_pos;
ANALYZE TABLE fii_dii_inv;
-- --------------------building database for nse cm---------------------------------

-- import
mysql --local-infile=1 -uroot -p
Mypass@123
SET GLOBAL local_infile = 1;

-- ---------------------create dummy table -----------------------------------------
create table dummy (rn int);
insert into dummy
select rn + 50000 from dummy

create table d_data 
(
 c1 	varchar(100),
 c2 	varchar(100),
 c3 	varchar(100),
 c4 	varchar(100),
 c5 	varchar(100),
 c6 	varchar(100),
 c7 	varchar(100),
 c8 	varchar(100),
 c9 	varchar(100),
 c10 	varchar(100)
)

-- ------------------date table creation process---------------------
-- date and url table
drop table m_date 
create table m_date 
( mk_date 	date,
  cm_trade_day	char(1),
   sno int
)

create unique index idx_m_date_mk_date on m_date(mk_date)

insert into m_date 
select date_add(str_to_date('31-12-1993', '%d-%m-%Y'), interval rn day), 'N', 0 from dummy 
where date_add(str_to_date('01-01-1994', '%d-%m-%Y'), interval rn day) <= str_to_date('03-11-1994', '%d-%m-%Y')

insert into m_date 
select date_add(str_to_date('02-11-1994', '%d-%m-%Y'), interval rn day), 'Y' from dummy 
where date_add(str_to_date('02-11-1994', '%d-%m-%Y'), interval rn day) <= str_to_date('31-12-2020', '%d-%m-%Y')

update m_date m set cm_trade_day = 'N' where not exists
(select 1 from eqty_price_vol e where e.timestamp = m.mk_date)

update m_date m set cm_trade_day = 'Y' where mk_Date >= str_to_date('01-06-2020', '%d-%m-%Y')

update m_date m set cm_trade_day = 'N' where UPPER(SUBSTR(dayname(mk_date), 1,3)) in ('SAT', 'SUN')

update m_date set cm_trade_day = 'N' where mk_date in 
(str_to_date('21-Feb-2020', '%d-%M-%Y'),
str_to_date('10-Mar-2020', '%d-%M-%Y'),
str_to_date('02-Apr-2020', '%d-%M-%Y'),
str_to_date('06-Apr-2020', '%d-%M-%Y'),
str_to_date('10-Apr-2020', '%d-%M-%Y'),
str_to_date('14-Apr-2020', '%d-%M-%Y'),
str_to_date('01-May-2020', '%d-%M-%Y'),
str_to_date('25-May-2020', '%d-%M-%Y'),
str_to_date('02-Oct-2020', '%d-%M-%Y'),
str_to_date('16-Nov-2020', '%d-%M-%Y'),
str_to_date('30-Nov-2020', '%d-%M-%Y'))

-- yearly activity
insert into m_date 
select date_add(str_to_date('31-12-2020', '%d-%m-%Y'), interval rn day), 'Y' from dummy
where rn <= 365

update m_date m set cm_trade_day = 'N' where UPPER(SUBSTR(dayname(mk_date), 1,3)) in ('SAT', 'SUN') and 
mk_Date >= str_to_date('01-01-2021', '%d-%m-%Y')

update m_date set cm_trade_day = 'N' where mk_date in 
(str_to_date('26-Jan-2021', '%d-%M-%Y'),
str_to_date('11-Mar-2021', '%d-%M-%Y'),
str_to_date('29-Mar-2021', '%d-%M-%Y'),
str_to_date('02-Apr-2021', '%d-%M-%Y'),
str_to_date('14-Apr-2021', '%d-%M-%Y'),
str_to_date('21-Apr-2021', '%d-%M-%Y'),
str_to_date('13-May-2021', '%d-%M-%Y'),
str_to_date('21-Jul-2021', '%d-%M-%Y'),
str_to_date('19-Aug-2021', '%d-%M-%Y'),
str_to_date('10-Sep-2021', '%d-%M-%Y'),
str_to_date('15-Oct-2021', '%d-%M-%Y'),
str_to_date('04-Nov-2021', '%d-%M-%Y'),
str_to_date('05-Nov-2021', '%d-%M-%Y'),
str_to_date('19-Nov-2021', '%d-%M-%Y'))

update m_date set cm_trade_day = 'Y' where mk_Date = str_to_date( '28-02-2015' , '%d-%m-%Y');
update m_date set cm_trade_day = 'Y' where mk_Date = str_to_date( '30-10-2016' , '%d-%m-%Y');
update m_date set cm_trade_day = 'Y' where mk_Date = str_to_date( '27-10-2019' , '%d-%m-%Y');
update m_date set cm_trade_day = 'Y' where mk_Date = str_to_date( '01-02-2020' , '%d-%m-%Y');
update m_date set cm_trade_day = 'Y' where mk_date in 
(str_to_date('1995-09-04', '%Y-%m-%d'),
str_to_date('1996-10-30', '%Y-%m-%d'),
str_to_date('1996-11-10', '%Y-%m-%d'),
str_to_date('1997-03-01', '%Y-%m-%d'),
str_to_date('1997-04-12', '%Y-%m-%d'),
str_to_date('1998-10-31', '%Y-%m-%d'),
str_to_date('1998-11-21', '%Y-%m-%d'),
str_to_date('1998-11-28', '%Y-%m-%d'),
str_to_date('1999-02-27', '%Y-%m-%d'),
str_to_date('1999-03-20', '%Y-%m-%d'),
str_to_date('1999-04-17', '%Y-%m-%d'),
str_to_date('1999-10-23', '%Y-%m-%d'),
str_to_date('1999-11-07', '%Y-%m-%d'));

update m_date set cm_trade_day = 'Y' where mk_date in 
(str_to_date('1994-01-03', '%Y-%m-%d'),
str_to_date('1994-01-04', '%Y-%m-%d'),
str_to_date('1994-01-05', '%Y-%m-%d'),
str_to_date('1994-01-06', '%Y-%m-%d'),
str_to_date('1994-01-07', '%Y-%m-%d'),
str_to_date('1994-01-10', '%Y-%m-%d'),
str_to_date('1994-01-11', '%Y-%m-%d'),
str_to_date('1994-01-12', '%Y-%m-%d'),
str_to_date('1994-01-13', '%Y-%m-%d'),
str_to_date('1994-01-14', '%Y-%m-%d'),
str_to_date('1994-01-17', '%Y-%m-%d'),
str_to_date('1994-01-18', '%Y-%m-%d'),
str_to_date('1994-01-19', '%Y-%m-%d'),
str_to_date('1994-01-20', '%Y-%m-%d'),
str_to_date('1994-01-24', '%Y-%m-%d'),
str_to_date('1994-01-25', '%Y-%m-%d'),
str_to_date('1994-01-27', '%Y-%m-%d'),
str_to_date('1994-01-28', '%Y-%m-%d'),
str_to_date('1994-01-31', '%Y-%m-%d'),
str_to_date('1994-02-01', '%Y-%m-%d'),
str_to_date('1994-02-02', '%Y-%m-%d'),
str_to_date('1994-02-03', '%Y-%m-%d'),
str_to_date('1994-02-04', '%Y-%m-%d'),
str_to_date('1994-02-07', '%Y-%m-%d'),
str_to_date('1994-02-08', '%Y-%m-%d'),
str_to_date('1994-02-09', '%Y-%m-%d'),
str_to_date('1994-02-10', '%Y-%m-%d'),
str_to_date('1994-02-11', '%Y-%m-%d'),
str_to_date('1994-02-14', '%Y-%m-%d'),
str_to_date('1994-02-15', '%Y-%m-%d'),
str_to_date('1994-02-16', '%Y-%m-%d'),
str_to_date('1994-02-17', '%Y-%m-%d'),
str_to_date('1994-02-18', '%Y-%m-%d'),
str_to_date('1994-02-21', '%Y-%m-%d'),
str_to_date('1994-02-22', '%Y-%m-%d'),
str_to_date('1994-02-23', '%Y-%m-%d'),
str_to_date('1994-02-24', '%Y-%m-%d'),
str_to_date('1994-02-25', '%Y-%m-%d'),
str_to_date('1994-02-28', '%Y-%m-%d'),
str_to_date('1994-03-01', '%Y-%m-%d'),
str_to_date('1994-03-02', '%Y-%m-%d'),
str_to_date('1994-03-03', '%Y-%m-%d'),
str_to_date('1994-03-04', '%Y-%m-%d'),
str_to_date('1994-03-07', '%Y-%m-%d'),
str_to_date('1994-03-08', '%Y-%m-%d'),
str_to_date('1994-03-09', '%Y-%m-%d'),
str_to_date('1994-03-11', '%Y-%m-%d'),
str_to_date('1994-03-15', '%Y-%m-%d'),
str_to_date('1994-03-16', '%Y-%m-%d'),
str_to_date('1994-03-17', '%Y-%m-%d'),
str_to_date('1994-03-18', '%Y-%m-%d'),
str_to_date('1994-03-21', '%Y-%m-%d'),
str_to_date('1994-03-22', '%Y-%m-%d'),
str_to_date('1994-03-23', '%Y-%m-%d'),
str_to_date('1994-03-24', '%Y-%m-%d'),
str_to_date('1994-03-25', '%Y-%m-%d'),
str_to_date('1994-03-28', '%Y-%m-%d'),
str_to_date('1994-03-29', '%Y-%m-%d'),
str_to_date('1994-03-30', '%Y-%m-%d'),
str_to_date('1994-03-31', '%Y-%m-%d'),
str_to_date('1994-04-04', '%Y-%m-%d'),
str_to_date('1994-04-05', '%Y-%m-%d'),
str_to_date('1994-04-06', '%Y-%m-%d'),
str_to_date('1994-04-07', '%Y-%m-%d'),
str_to_date('1994-04-08', '%Y-%m-%d'),
str_to_date('1994-04-12', '%Y-%m-%d'),
str_to_date('1994-04-13', '%Y-%m-%d'),
str_to_date('1994-04-15', '%Y-%m-%d'),
str_to_date('1994-04-18', '%Y-%m-%d'),
str_to_date('1994-04-19', '%Y-%m-%d'),
str_to_date('1994-04-21', '%Y-%m-%d'),
str_to_date('1994-04-22', '%Y-%m-%d'),
str_to_date('1994-04-25', '%Y-%m-%d'),
str_to_date('1994-04-26', '%Y-%m-%d'),
str_to_date('1994-04-27', '%Y-%m-%d'),
str_to_date('1994-04-28', '%Y-%m-%d'),
str_to_date('1994-04-29', '%Y-%m-%d'),
str_to_date('1994-05-02', '%Y-%m-%d'),
str_to_date('1994-05-03', '%Y-%m-%d'),
str_to_date('1994-05-04', '%Y-%m-%d'),
str_to_date('1994-05-05', '%Y-%m-%d'),
str_to_date('1994-05-06', '%Y-%m-%d'),
str_to_date('1994-05-09', '%Y-%m-%d'),
str_to_date('1994-05-10', '%Y-%m-%d'),
str_to_date('1994-05-11', '%Y-%m-%d'),
str_to_date('1994-05-13', '%Y-%m-%d'),
str_to_date('1994-05-16', '%Y-%m-%d'),
str_to_date('1994-05-17', '%Y-%m-%d'),
str_to_date('1994-05-18', '%Y-%m-%d'),
str_to_date('1994-05-19', '%Y-%m-%d'),
str_to_date('1994-05-20', '%Y-%m-%d'),
str_to_date('1994-05-23', '%Y-%m-%d'),
str_to_date('1994-05-24', '%Y-%m-%d'),
str_to_date('1994-05-26', '%Y-%m-%d'),
str_to_date('1994-05-27', '%Y-%m-%d'),
str_to_date('1994-05-30', '%Y-%m-%d'),
str_to_date('1994-05-31', '%Y-%m-%d'),
str_to_date('1994-06-01', '%Y-%m-%d'),
str_to_date('1994-06-02', '%Y-%m-%d'),
str_to_date('1994-06-03', '%Y-%m-%d'),
str_to_date('1994-06-06', '%Y-%m-%d'),
str_to_date('1994-06-07', '%Y-%m-%d'),
str_to_date('1994-06-08', '%Y-%m-%d'),
str_to_date('1994-06-09', '%Y-%m-%d'),
str_to_date('1994-06-10', '%Y-%m-%d'),
str_to_date('1994-06-13', '%Y-%m-%d'),
str_to_date('1994-06-14', '%Y-%m-%d'),
str_to_date('1994-06-15', '%Y-%m-%d'),
str_to_date('1994-06-16', '%Y-%m-%d'),
str_to_date('1994-06-17', '%Y-%m-%d'),
str_to_date('1994-06-20', '%Y-%m-%d'),
str_to_date('1994-06-22', '%Y-%m-%d'),
str_to_date('1994-06-23', '%Y-%m-%d'),
str_to_date('1994-06-24', '%Y-%m-%d'),
str_to_date('1994-06-27', '%Y-%m-%d'),
str_to_date('1994-06-28', '%Y-%m-%d'),
str_to_date('1994-06-29', '%Y-%m-%d'),
str_to_date('1994-06-30', '%Y-%m-%d'),
str_to_date('1994-07-01', '%Y-%m-%d'),
str_to_date('1994-07-04', '%Y-%m-%d'),
str_to_date('1994-07-05', '%Y-%m-%d'),
str_to_date('1994-07-06', '%Y-%m-%d'),
str_to_date('1994-07-07', '%Y-%m-%d'),
str_to_date('1994-07-08', '%Y-%m-%d'),
str_to_date('1994-07-11', '%Y-%m-%d'),
str_to_date('1994-07-12', '%Y-%m-%d'),
str_to_date('1994-07-13', '%Y-%m-%d'),
str_to_date('1994-07-14', '%Y-%m-%d'),
str_to_date('1994-07-15', '%Y-%m-%d'),
str_to_date('1994-07-18', '%Y-%m-%d'),
str_to_date('1994-07-19', '%Y-%m-%d'),
str_to_date('1994-07-20', '%Y-%m-%d'),
str_to_date('1994-07-21', '%Y-%m-%d'),
str_to_date('1994-07-22', '%Y-%m-%d'),
str_to_date('1994-07-25', '%Y-%m-%d'),
str_to_date('1994-07-26', '%Y-%m-%d'),
str_to_date('1994-07-27', '%Y-%m-%d'),
str_to_date('1994-07-28', '%Y-%m-%d'),
str_to_date('1994-07-29', '%Y-%m-%d'),
str_to_date('1994-08-01', '%Y-%m-%d'),
str_to_date('1994-08-02', '%Y-%m-%d'),
str_to_date('1994-08-03', '%Y-%m-%d'),
str_to_date('1994-08-04', '%Y-%m-%d'),
str_to_date('1994-08-05', '%Y-%m-%d'),
str_to_date('1994-08-08', '%Y-%m-%d'),
str_to_date('1994-08-09', '%Y-%m-%d'),
str_to_date('1994-08-10', '%Y-%m-%d'),
str_to_date('1994-08-11', '%Y-%m-%d'),
str_to_date('1994-08-12', '%Y-%m-%d'),
str_to_date('1994-08-16', '%Y-%m-%d'),
str_to_date('1994-08-17', '%Y-%m-%d'),
str_to_date('1994-08-18', '%Y-%m-%d'),
str_to_date('1994-08-19', '%Y-%m-%d'),
str_to_date('1994-08-24', '%Y-%m-%d'),
str_to_date('1994-08-25', '%Y-%m-%d'),
str_to_date('1994-08-29', '%Y-%m-%d'),
str_to_date('1994-08-31', '%Y-%m-%d'),
str_to_date('1994-09-01', '%Y-%m-%d'),
str_to_date('1994-09-02', '%Y-%m-%d'),
str_to_date('1994-09-05', '%Y-%m-%d'),
str_to_date('1994-09-06', '%Y-%m-%d'),
str_to_date('1994-09-07', '%Y-%m-%d'),
str_to_date('1994-09-08', '%Y-%m-%d'),
str_to_date('1994-09-12', '%Y-%m-%d'),
str_to_date('1994-09-13', '%Y-%m-%d'),
str_to_date('1994-09-14', '%Y-%m-%d'),
str_to_date('1994-09-15', '%Y-%m-%d'),
str_to_date('1994-09-16', '%Y-%m-%d'),
str_to_date('1994-09-19', '%Y-%m-%d'),
str_to_date('1994-09-20', '%Y-%m-%d'),
str_to_date('1994-09-21', '%Y-%m-%d'),
str_to_date('1994-09-22', '%Y-%m-%d'),
str_to_date('1994-09-23', '%Y-%m-%d'),
str_to_date('1994-09-27', '%Y-%m-%d'),
str_to_date('1994-09-28', '%Y-%m-%d'),
str_to_date('1994-09-29', '%Y-%m-%d'),
str_to_date('1994-09-30', '%Y-%m-%d'),
str_to_date('1994-10-03', '%Y-%m-%d'),
str_to_date('1994-10-04', '%Y-%m-%d'),
str_to_date('1994-10-05', '%Y-%m-%d'),
str_to_date('1994-10-06', '%Y-%m-%d'),
str_to_date('1994-10-07', '%Y-%m-%d'),
str_to_date('1994-10-10', '%Y-%m-%d'),
str_to_date('1994-10-11', '%Y-%m-%d'),
str_to_date('1994-10-12', '%Y-%m-%d'),
str_to_date('1994-10-14', '%Y-%m-%d'),
str_to_date('1994-10-17', '%Y-%m-%d'),
str_to_date('1994-10-18', '%Y-%m-%d'),
str_to_date('1994-10-19', '%Y-%m-%d'),
str_to_date('1994-10-20', '%Y-%m-%d'),
str_to_date('1994-10-21', '%Y-%m-%d'),
str_to_date('1994-10-24', '%Y-%m-%d'),
str_to_date('1994-10-25', '%Y-%m-%d'),
str_to_date('1994-10-26', '%Y-%m-%d'),
str_to_date('1994-10-27', '%Y-%m-%d'),
str_to_date('1994-10-28', '%Y-%m-%d'),
str_to_date('1994-10-31', '%Y-%m-%d'));

update m_date set sno = 0;
drop procedure pr_sno; 
delimiter //
CREATE PROCEDURE pr_sno()
BEGIN 

declare c_mk_date date;
declare c_sno int default  1;
declare is_done int default 0;

declare cur_sno cursor for 
select mk_Date from m_date where cm_trade_day = 'Y' order by mk_date;

declare continue handler for not found set is_done = 1;
set autocommit = 0 ;
open cur_sno;
label: loop
fetch cur_sno into c_mk_date; 

update m_date set sno = c_sno where mk_date = c_mk_date;
set c_sno = c_sno + 1;

if is_done = 1 then
leave label;
end if;

end loop label;
close cur_sno;
commit;
end;//
DELIMITER;

call pr_sno()

-- ------------------URL table creation process---------------------
drop table md_url
create table md_url
(
MD_TYPE			VARCHAR(4),
md_freq			VARCHAR(1),
URL				VARCHAR(250),
filemask		VARCHAR(50),
DL_RLTV_PATH	VARCHAR(100),
active			varchar(1)
)

delete from md_url
insert into md_url values ('E', 'D', 'https://archives.nseindia.com/content/historical/EQUITIES//YYYY/MON/', 'cmDDMONYYYYbhav.csv.zip', 'NSE\\DATA\\01_EQTY_PRICE_VOL\\', 'Y');
insert into md_url values ('I', 'D', 'https://archives.nseindia.com/content/indices/', 'ind_close_all_DDMMYYYY.csv', 'NSE\\DATA\\02_INDX_PRICE_VOL\\', 'Y');
insert into md_url values ('E', 'D', 'https://archives.nseindia.com/content/equities/', 'sec_list_DDMMYYYY.csv', 'NSE\\DATA\\03_M_EQTY\\', 'Y');
insert into md_url values ('E', 'D', 'https://archives.nseindia.com/content/equities/','series_change.csv', 'NSE\\DATA\\03_M_EQTY\\', 'Y');
insert into md_url values ('E', 'D', 'https://archives.nseindia.com/content/equities/', 'eq_band_changes_DDMMYYYY.csv', 'NSE\\DATA\\03_M_EQTY\\', 'Y');
insert into md_url values ('E', 'D', 'https://archives.nseindia.com/archives/equities/mto/', 'MTO_DDMMYYYY.DAT', 'NSE\\DATA\\04_EQTY_DLVRY_POS\\', 'Y');
insert into md_url values ('E', 'D', 'https://www1.nseindia.com/archives/equities/mto/', '', 'NSE\\DATA\\05_VOLATILITY\\', 'N');
insert into md_url values ('I', 'M', 'https://archives.nseindia.com/content/indices/', '', 'NSE\\DATA\\06_M_INDICES\\', 'N');

create table md_url_bkp 
as
select * from md_url

update md_url set active = 'Y' where filemask like 'sec_list_DDMMYYYY%'

-- security list
-- format mask
-- Security Price Vol 				---- https://www1.nseindia.com/content/historical/EQUITIES/1994/NOV/cm03NOV1994bhav.csv.zip
-- Security wise delivery position  ---- https://www1.nseindia.com/archives/equities/mto/MOT_29052020.DAT
-- Indices open close 				---- https://archives.nseindia.com/content/indices/ind_close_all_31122020.csv

-- build url for security prive volume 
select a, 'Y',  concat('https://www1.nseindia.com/content/historical/EQUITIES/', ot.y, '/', ot.mon, '/', 'cm', ot.dae, ot.mon, ot.y, 'bhav.csv.zip')
from 
(select a, 
	    year(a) y , 
	    upper(substr(monthname(a), 1, 3)) mon, 
        convert(lpad(day(a),2, 0), char) dae from 
(select mk_Date a from m_date where cm_trade_day = 'Y') it) ot

-- build url for indices open close 
-- https://arhives.nseindia.com/content/indices/ind_close_all_29052020.csv
select * from concat('https://archives.nseindia.com/content/indices/ind_close_all_', 
DATE_FORMAT(mk_date, "%d%m%Y"), '.csv')
where cm_trade_day = 'Y'

-- url for security wise delivery position 
-- https://www1.nseindia.com/archives/equities/mto/MOT_03111994.DAT 
select concat('https://www1.nseindia.com/archives/equities/mto/MOT_',  DATE_FORMAT(mk_date, "%d%m%Y"), '.DAT')
from m_date where cm_trade_day = 'Y'

-- url for price bank chnges 
-- https://www1.nseindia.com/content/equities/eq_bank_changes_DDMMYYYY.csv
select concat('https://www1.nseindia.com/content/equities/eq_bank_changes_',  DATE_FORMAT(mk_date, "%d%m%Y"), '.csv')
from m_date where cm_trade_day = 'Y'

-- duplicate date in maintenance table 
select mk_date, count(1) from m_date group by mk_date having count(1) > 1 ;

-- ----------------maintain core parameters ----------------------
create table m_core_params
(
param 			varchar(100),
param_value  	varchar(100)
)
delete from m_core_params;
insert into m_core_params values ('LAST_TR_DT', '18-JUN-2020');
insert into m_core_params values ('CURR_TR_DT', '19-JUN-2020');
insert into m_core_params values ('NEXT_TR_DT', '22-JUN-2020');
insert into m_core_params values ('HOME_PATH', 'E:\\Projects\\Sandbox\\');
select * from m_core_params;
-- ---------------------- maintain equities ----------------------------------------
create table st_m_equities 
(
Symbol			varchar(25),
Series			varchar(3),
Security_Name	varchar(200),
Band			int(3),
Remarks			varchar(100)
)
select * from m_equities
drop table m_equities
create table m_equities 
(
Symbol			varchar(25),
ISIN 			varchar(30),
Series			varchar(3),
prev_series		varchar(3),
Security_Name	varchar(200),
Band			int(3),
prev_band		int(3),
date_effect		date, 
Remarks			varchar(100)
)
create unique index indx_m_equities on m_equities (Symbol, series); 

LOAD DATA LOCAL INFILE 'E:\\Projects\\Sandbox\\NSE\\data\\m_equities\\sec_list.csv'
INTO TABLE st_m_equities
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Symbol, Series, Security_Name, band, Remarks)

-- 'PRICEBAND' 'SERIES'
insert into d_data (c1, c2,  c3, c4, c5, c6, c7) values ('SERIES', 'UMESLTD', 'Usha Martin Education & Solutions Limited', 'EQ', 'BE', '12-JUN-2020', 'GSM Stage - II');
insert into d_data (c1, c2,  c3, c4, c5, c6) VALUES ('PRICEBAND','AHLWEST', 'EQ', 'Asian Hotels (West) Limited', '10', '5');
insert into d_data (c1, c2,  c3, c4, c5, c6) VALUES ('PRICEBAND','ANKITMETAL', 'EQ', 'Ankit Metal & Power Limited', '10', '5');
insert into d_data (c1, c2,  c3, c4, c5, c6) VALUES ('PRICEBAND','MANAKALUCO', 'EQ', 'Manaksia Aluminium Company Limited', '10', '5');
insert into d_data (c1, c2,  c3, c4, c5, c6) VALUES ('PRICEBAND','TAINWALCHM', 'EQ', 'Tainwala Chemical and Plastic (I) Limited', '10', '5');
insert into d_data (c1, c2,  c3, c4, c5, c6) VALUES ('PRICEBAND','INTENTECH', 'EQ', 'Intense Technologies Limited', '20', '10');
insert into d_data (c1, c2,  c3, c4, c5, c6) VALUES ('PRICEBAND','JTEKTINDIA', 'EQ', 'Jtekt India Limited', '20', '10');
insert into d_data (c1, c2,  c3, c4, c5, c6) VALUES ('PRICEBAND','AARVEEDEN', 'EQ', 'Aarvee Denims & Exports Limited', '5', '10');
insert into d_data (c1, c2,  c3, c4, c5, c6) VALUES ('PRICEBAND','SHOPERSTOP', 'EQ', 'Shoppers Stop Limited', '5', '10');

insert into m_equities 
select s.symbol, e.isin, s.series, s.series, s.security_name, s.band, s.band, null,s.remarks from st_m_equities s INNER JOIN 
(select symbol, series, ISIN from eqty_price_vol where timestamp = str_to_date('12-06-2020', '%d-%m-%Y')) e 
ON s.symbol = e.symbol and s.series = e.series

update m_equities e inner join 
(select c2,c3, c4, c5 from d_data where c1 = 'SERIES') s on 
e.symbol = s.c2 and e.series = s.c3 
set e.prev_band = s.c4 , e.series = s.c5 , e.date_effect = str_to_date('15-06-2020', '%d-%m-%Y')

update m_equities e inner join 
(select c2, c3, c5, c6 from d_data where c1 = 'PRICEBAND') s on 
e.symbol = s.c2 and e.series = s.c3
set e.prev_band = s.c5, e.band = s.c6, e.date_effect = str_to_date('15-06-2020', '%d-%m-%Y')

select * from m_equities where symbol = 'AHLWEST'
select * from m_equities e inner join 
(select c2, c3, c5, c6 from d_data where c1 = 'PRICEBAND') s on 
e.symbol = s.c2 and e.series = s.c3

-- --------------------------- indices ---------------------------------------------
drop table m_indices
create table m_indices
(
index_type		varchar(25),
Indx			varchar(25),
Company_Name	varchar(250),
Industry		varchar(50),
Symbol			varchar(25),
Series			varchar(3),
ISIN 			varchar(25),
maint_date		date
)
create unique index idx_m_indices_ind_symbol on m_indices (indx, symbol)
create index idx_m_indices_symbol on m_indices (symbol)
create index idx_m_indices_indx on m_indices (indx)

LOAD DATA LOCAL INFILE 'E:\\Projects\\Sandbox\\NSE\\data\\m_data\\_broader_market_index.csv'
INTO TABLE m_indices
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(index_type, Indx, Company_Name, Industry, Symbol, Series, ISIN, @m_date)
SET maint_date = STR_TO_DATE(@m_date, '%d-%M-%Y');

UPDATE m_indices SET INDEX_TYPE = 'SECTORAL' WHERE INDEX_TYPE = 'SECTORAL_INDEX' 
UPDATE m_indices SET indx = 'NIFTYMETAL' WHERE indx = 'NIFTYMETALS' 
UPDATE m_indices SET indx = 'NIFTYOILANDGAS' WHERE indx = 'NIFTYOIL' 

delete from m_indices where INDX = 'INDEX'
select * from m_indices;

-- ----------------------- bulk import of all data bhavcopy from nse  ---------------------
SYMBOL	SERIES	OPEN	HIGH	LOW	CLOSE	LAST	PREVCLOSE	TOTTRDQTY	TOTTRDVAL	TIMESTAMP	TOTALTRADES	ISIN

DROP TABLE st_eqty_price_vol
create table st_eqty_price_vol
(
SYMBOL				VARCHAR(50),
SERIES				VARCHAR(4),
OPEN				DECIMAL(12,2),
HIGH				DECIMAL(12,2),
LOW					DECIMAL(12,2),
CLOSE				DECIMAL(12,2),
LAST				DECIMAL(12,2),	
PREVCLOSE			DECIMAL(12,2),
TOTTRDQTY			BIGINT(20),
TOTTRDVAL			decimal(20,2),
TIMESTAMP			VARCHAR(30),
TOTALTRADES			INT(20),
ISIN				VARCHAR(30)
)

drop table eqty_price_vol
create table eqty_price_vol
(
SYMBOL				VARCHAR(50),
SERIES				VARCHAR(4),
OPEN				DECIMAL(12,2),
HIGH				DECIMAL(12,2),
LOW					DECIMAL(12,2),
CLOSE				DECIMAL(12,2),
LAST				DECIMAL(12,2),
PREVCLOSE			DECIMAL(12,2),
TOTTRDQTY			BIGINT(20),
TOTTRDVAL			DECIMAL(20,2),
TIMESTAMP			date,
TOTALTRADES			INT(20),
ISIN				VARCHAR(30)
)

drop index idx_eqty_price_vol on eqty_price_vol
drop index idx_eqty_price_vol_sym on eqty_price_vol

create UNIQUE index idx_eqty_time_sst ON eqty_price_vol (SYMBOL, SERIES, TIMESTAMP)
CREATE INDEX idx_eqty_time ON eqty_price_vol (TIMESTAMP)
create index idx_eqty_price_vol on eqty_price_vol (symbol, series, close)
create index idx_eqty_price_vol_sym on eqty_price_vol (symbol)

LOAD DATA LOCAL INFILE 'E:\\Projects\\Sandbox\\NSE\\data\\01_sec_price_vol\\X\\unzip\\_all.csv'
INTO TABLE st_eqty_price_vol 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS 
(SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN);
-- 

-- bulk insert one time acticvity
INSERT INTO eqty_price_vol
select SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,str_to_date(timestamp, '%d-%M-%Y'),TOTALTRADES,ISIN 
FROM st_eqty_price_vol;

cd D:\MyDir;
copy /b *.DAT _all.DAT;
copy /b *.csv _all.DAT;

-- --------------- historic indices open close -----------------------------
drop table st_indx_price_vol
create table st_indx_price_vol
(
indx 		VARCHAR(100),
mk_date		date,
open		DECIMAL(12,2),
high		DECIMAL(12,2),
low			DECIMAL(8,2),
close		DECIMAL(8,2),
chnge		DECIMAL(8,2),
p_change	DECIMAL(8,2),
volume		BIGINT(20),
turnover	DECIMAL(22,2),
PE			DECIMAL(8,2),
PB			DECIMAL(8,2),
Yield		DECIMAL(8,2)
)

drop table indx_price_vol
create table indx_price_vol
(
indx 		VARCHAR(100),
mk_date		date,
open		DECIMAL(12,2),
high		DECIMAL(12,2),
low			DECIMAL(12,2),
close		DECIMAL(12,2),
chnge		DECIMAL(12,2),
p_change	DECIMAL(12,2),
volume		BIGINT(20),
turnover	DECIMAL(22,2),
PE			DECIMAL(12,2),
PB			DECIMAL(12,2),
Yield		DECIMAL(12,2)
)
create unique index indx_indx_price_vol on indx_price_vol (indx, mk_date)

truncate table st_indx_price_vol;
LOAD DATA LOCAL INFILE 'E:\\Projects\\Sandbox\\NSE\\data\\02_historic_indices_open_close\\v3\\_all.csv'
INTO TABLE st_indx_price_vol
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Indx, @m_date, open, high, low, close, chnge, p_change, volume, turnover, PE, PB, Yield)
SET mk_date = STR_TO_DATE(@m_date, '%d-%m-%Y');

update st_indx_price_vol set indx = upper(replace(indx, ' ', ''));
delete from st_indx_price_vol where mk_date = '0000-00-00';
delete from st_indx_price_vol where indx = 'CNXALPHAINDEX' and mk_date = str_to_date('08-02-2013', '%d-%m-%Y')
delete from st_indx_price_vol where indx = 'GSEC10NSEINDEX' and mk_date = str_to_date('15-09-2014', '%d-%m-%Y')
delete from st_indx_price_vol where  indx = 'INDEXNAME';
update st_indx_price_vol set turnover = turnover * 10000000;
update st_indx_price_vol set indx = 'NIFTYFINANCE' where indx = 'NIFTYFINANCIALSERVICES' 
update st_indx_price_vol set indx = 'NIFTYOILANDGAS' where indx = 'NIFTYOIL&GAS'

insert into indx_price_vol
select indx, mk_date, open, high, low, close, chnge, p_change, volume, turnover, PE, PB, Yield
from st_indx_price_vol; -- 118334 

update st_indx_price_vol_2 set indx = 'NIFTYOILANDGAS' where indx = 'NIFTYOIL' 
delete from st_indx_price_vol_2 where mk_Date = str_to_date('20-04-2020', '%d-%m-%Y')-- '2012-02-21'  
DELETE FROM st_indx_price_vol_2 WHERE indx = 'NIFTYPHARMA' and mk_Date = str_to_date('30-12-2004', '%d-%m-%Y')
insert into st_indx_price_vol_2 values ( 'SECTORAL', 'NIFTYPHARMA', str_to_date('30-12-2004', '%d-%m-%Y'), 0,0,0, 2169.42, 0, 0)

insert into indx_price_vol 
select indx, mk_Date, open, high, low, close, null, null, shares_traded, turnover, null, null, null 
from st_indx_price_vol_2 where (indx, mk_date) not in 
(select i.indx, i.mk_date from indx_price_vol i inner join st_indx_price_vol_2 s 
on i.indx = s.indx and i.mk_date = s.mk_date)

select indx, mk_date, count(1) from st_indx_price_vol group by indx, mk_date having count(1) > 1; -- zero rows returned 
-- ------------------- price band chances ------------------------
drop table 

-- --------- histroic delivery position ---------------------------
drop table st_eqty_dlvry_pos 
create table st_eqty_dlvry_pos 
(
mk_date 		date,
record_type 	varchar(3),
symbol			varchar(25),
series			varchar(3),
qty_traded		bigint(20),
dlvr_qty		bigint(20),
dlvr_prcnt		decimal(5,2)
)

create table eqty_dlvry_pos 
(
mk_date 		date,
record_type 	varchar(3),
symbol			varchar(25),
series			varchar(3),
qty_traded		bigint(20),
dlvr_qty		bigint(20),
dlvr_prcnt		decimal(5,2)
)
create UNIQUE index indx_eqty_dlvry_pos ON eqty_dlvry_pos (mk_date, symbol, series)

TRUNCATE TABLE st_eqty_dlvry_pos;

LOAD DATA LOCAL INFILE 'E:\\Projects\\Sandbox\\NSE\\data\\04_security_wise_delivery_position\\b\\_all.csv'
INTO TABLE st_eqty_dlvry_pos
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@m_date, record_type, symbol, series, qty_traded, dlvr_qty, dlvr_prcnt)
SET mk_date = STR_TO_DATE(lpad(@m_date, 8, '0'), '%d%m%Y');

select count(1) from st_eqty_dlvry_pos; 5725260 
select count(1) from st_eqty_dlvry_pos where mk_Date is null;
select count(1) from st_eqty_dlvry_pos where qty_traded = 2147483647;
select count(1) from st_eqty_dlvry_pos where dlvr_qty = 2147483647;
select * from st_eqty_dlvry_pos group by mk_date, symbol, series having count(1) > 1

insert into eqty_dlvry_pos 
select * from st_eqty_dlvry_pos where mk_date >=  str_to_date('01-06-2020', '%d-%m-%Y')

select MAX(mk_date) from st_eqty_dlvry_pos;
select count(1) from eqty_dlvry_pos; -- 

-- ---------fii dii----------------------------
create table st_fii_dii_inv
(
mk_date			varchar(20),
f_gross_buy		decimal(25,2),
f_gross_sell	decimal(25,2),
f_net			decimal(25,2),
d_gross_buy		decimal(25,2),
d_gross_sell	decimal(25,2),
d_net			decimal(25,2)
)
create table fii_dii_inv
(
mk_date			date,
f_gross_buy		decimal(25,2),
f_gross_sell	decimal(25,2),
f_net			decimal(25,2),
d_gross_buy		decimal(25,2),
d_gross_sell	decimal(25,2),
d_net			decimal(25,2)
)
create UNIQUE index idx_fii_dii_inv_01 ON  fii_dii_inv (mk_date)

LOAD DATA LOCAL INFILE 'E:\\Projects\\Sandbox\\NSE\\DATA\\07_FII_DII\\FII_DII_test.csv'
INTO TABLE st_fii_dii_inv
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(mk_Date, f_gross_buy, f_gross_sell, f_net, d_gross_buy, d_gross_sell, d_net);
SET mk_date = STR_TO_DATE(@m_date, '%d-%M-%Y');
SET maint_date = STR_TO_DATE(@m_date, '%d-%M-%Y');

insert into fii_dii_inv 
select str_to_date(mk_date, '%d-%M-%Y') , f_gross_buy, f_gross_sell, f_net, f_gross_buy, d_gross_sell, d_net from st_fii_dii_inv 

select count(1) from st_fii_dii_inv where f_net > 0 
select count(1) from fii_dii_inv where f_net > 0 
select * from st_fii_dii_inv order by str_to_date(mk_date, '%d-%M-%Y')  desc 
select * from fii_dii_inv group by mk_date having count(1) > 1 
truncate table st_fii_dii_inv 
truncate table fii_dii_inv 

drop table st_fii_dii_inv;

-- ---------portfolio table--------------------
create table curr_pf
(
SYMBOL				VARCHAR(50),
SERIES				VARCHAR(4),
ISIN				VARCHAR(30)
)

-- -------------------- error log -------------------------
create table error_log
(
proc		varchar(50),
err			varchar(250),
timestamp 	timestamp
)

-- -------------- process log ----------------------------
drop table process_log
create table process_log
(
process_id 	int,
date_time	varchar(40),
log			varchar(1000)
)

-- --------------- data migration procedures --------------

drop procedure pr_del_prev_data;

DELIMITER //
create procedure pr_del_prev_data()
begin
	delete from st_eqty_price_vol where timestamp = (select PARAM_VALUE from m_core_params where param = 'CURR_TR_DT');
    delete from st_indx_price_vol where mk_Date = (select str_to_date(PARAM_VALUE, '%d-%M-%Y') from m_core_params where param = 'CURR_TR_DT');
	delete from st_m_equities; 
	delete from d_data where c1 in ('PRICEBAND', 'SERIES');
	delete from st_eqty_dlvry_pos where mk_Date = (select str_to_date(PARAM_VALUE, '%d-%M-%Y'f) from m_core_params where param = 'CURR_TR_DT');
end ; //
DELIMITER ;

drop procedure pr_datatransfer;

delimiter //
CREATE PROCEDURE pr_datatransfer(IN p_nxt_date DATE, IN  p_proc_id INT)
BEGIN 

-- SERIES CHANGE
update m_equities m inner join d_data d on m.Symbol = d.c2 and m.Series = d.c4 
set m.prev_series = m.series, m.series = d.c5, m.date_effect= d.c6
where d.c1 = 'SERIES';
insert into process_log values (p_proc_id, current_timestamp(), 'series change updated...');

-- PRICE BAND CHANGE
update m_equities m inner join d_data d on m.Symbol = d.c2 and m.Series = d.c3
set m.prev_band = m.band, m.band = c6 
where d.c1 = 'PRICEBAND';
insert into process_log values (p_proc_id, current_timestamp(), 'price band updated...');

-- EQUITY PRICE VOLUME 
INSERT INTO eqty_price_vol 
select SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL, str_to_date(timestamp, '%d-%M-%Y'), TOTALTRADES,ISIN 
FROM st_eqty_price_vol s where str_to_date(timestamp, '%d-%M-%Y') = p_nxt_date; 
insert into process_log values (p_proc_id, current_timestamp(), 'equity price volume inserted...');

-- INDEX PRICE VOLUME
delete from st_indx_price_vol where mk_date = '0000-00-00';                                        
update st_indx_price_vol set indx = upper(replace(indx, ' ', ''));
delete from st_indx_price_vol where  indx = 'INDEXNAME';
update st_indx_price_vol set turnover = turnover * 10000000 ;
update st_indx_price_vol set indx = 'NIFTYFINANCE' where indx = 'NIFTYFINANCIALSERVICES' ;
update st_indx_price_vol set indx = 'NIFTYOILANDGAS' where indx = 'NIFTYOIL&GAS' ;

insert into indx_price_vol 
select indx, mk_date, open, high, low, close, chnge, p_change, volume, turnover, PE, PB, Yield 
from st_indx_price_vol  where mk_date = p_nxt_date; 

insert into process_log values (p_proc_id, current_timestamp(), 'index price volume inserted...');

-- EQUITY DELIVERY POSITION
insert into eqty_dlvry_pos 
select * from st_eqty_dlvry_pos where mk_date = p_nxt_date;
insert into process_log values (p_proc_id, current_timestamp(), 'delivery position inserted...');

-- maintenance 
select param_value into @l_curdt from m_core_params where param = 'CURR_TR_DT';
select param_value into @l_nxtdt from m_core_params where param = 'NEXT_TR_DT';

UPDATE m_core_params SET PARAM_VALUE = @l_curdt WHERE PARAM = 'LAST_TR_DT';
UPDATE m_core_params SET PARAM_VALUE = @l_nxtdt WHERE PARAM = 'CURR_TR_DT'; 
UPDATE m_core_params SET PARAM_VALUE = (select upper(date_format(min(mk_date), '%d-%b-%Y')) from m_date where mk_date > p_nxt_date and cm_trade_day = 'Y')
WHERE PARAM = 'NEXT_TR_DT';

insert into process_log values (p_proc_id, current_timestamp(), 'maintenance updated...');

END ; //
delimiter ;

call pr_del_prev_data()
call pr_datatransfer((select str_to_date(PARAM_VALUE, '%d-%M-%Y') from m_core_params where param = 'NEXT_TR_DT'), 280)
select * from process_log order by 2 desc
-- ----------------------------------------------------------------------------------------------------------------------------
