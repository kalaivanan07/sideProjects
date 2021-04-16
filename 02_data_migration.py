from     datetime import datetime 
import   os
import   shutil
import   csv 
import   mysql.connector
from     mysql.connector.errors import Error
import   urllib.request
from     urllib.request import HTTPError
import   zipfile 
from     zipfile import ZipFile 
import   zlib

def log_process(con, msg):
    try:
        sql = "insert into process_log (process_id, date_time, log) values (%s, %s, %s)"
        rs = (proc_id,  datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"), msg)
        cr = con.cursor() 
        cr.execute('set autocommit=0;')
        cr.execute(sql, rs)
        cr.execute('commit;')
        cr.close()

    except Error as e:        
        print(e)
    except Exception as e:
        error_log(con, 'log_process', e, datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))

def dbcon():
    try:
        global con
        con = mysql.connector.connect(host = 'localhost',
                           database = 'nsecm',
                          user     = 'root',
                          password = 'Mypass@123')
    except Exception as e:
        error_log(con, 'error_log', e, datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))
        if con.is_connected():
            con.close()
            print('connection is closed')        
        return False
    else:
        print('db connection obtained..')
        return True

def error_log(con, proc, err, date_time):
    try:
        sql = 'insert into error_log (proc, err, timestamp) values (%s, %s, %s)'
        val = (proc, err, date_time )
        cr = con.cursor()
        cr.execute('set autocommit=0;')
        cr.execute(sql, val)
        cr.execute('commit;')
        cr.close()
    except Exception as e:
        error_log(con, 'error_log', e, datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))
        if con.is_connected():
            con.close()
            print('connection is closed')        
        return False
    else:
        print('error logged..')
        return True

def zipUnzip(action, filename, path):
    try:
        if action == 'UNZIP':
            with ZipFile(filename, 'r') as zip:
                # extracting all the files
                log_process(con, 'Extracting all the files now...')
                print('Extracting all the files now...')
                zip.extract(filename[:filename.find('.zip')], path=path)
        elif action == 'ZIP':
            with ZipFile(filename+'.zip','w') as zip:
                # writing each file one by one 
                zip.write(filename, compress_type=zipfile.ZIP_DEFLATED)
    except Exception as e:
        print('Exception raised..', e)
        error_log(con, 'zipUnzip', e, datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f")) 
        if con.is_connected():
            con.close()
            print('connection is closed')
        return False
    else:
        log_process(con, 'zipunzip completed..')
        print('zipunzip completed..')
        return True            

def file_process(action):
    try:
        for eac in range(len(ls_data_path)):
            if action == 'PRE':
                os.chdir(home_path+ls_data_path[eac][2])
                log_process(con, home_path+ls_data_path[eac][2] + ':' +ls_data_path[eac][1])
                print(home_path+ls_data_path[eac][2], ls_data_path[eac][1])
                if ls_data_path[eac][1].find('.zip') > -1:
                    zipUnzip('UNZIP', ls_data_path[eac][1], home_path+ls_data_path[eac][2])
                    os.remove(home_path+ls_data_path[eac][2]+ls_data_path[eac][1])
            elif action == 'POST':
                os.chdir(home_path+ls_data_path[eac][2])
                filename = ls_data_path[eac][1]
                if ls_data_path[eac][1].find('.zip') > -1:
                    filename = filename[:filename.find('.zip')]
                zipUnzip('ZIP', filename, home_path+ls_data_path[eac][2])
                log_process(con, home_path+ls_data_path[eac][2] + filename + '.zip')
                print(home_path+ls_data_path[eac][2] + filename + '.zip')
                shutil.move(os.path.join(home_path+ls_data_path[eac][2], filename + '.zip'), os.path.join(home_path+ls_data_path[eac][2]+'History', filename + '.zip'))
                os.remove(home_path+ls_data_path[eac][2]+filename)
                
    except Exception as e:
        print('Exception raised ..', e)
        error_log(con, 'file_process', e, datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))
        if con.is_connected():
            con.close()
            print('connection is closed')
        return False
    else:
        log_process(con, 'file_process completed..')
        print('file_process completed..')
        return True

def data_insert(con, sql, records_to_insert):
    try:
        cr = con.cursor()
        cr.execute('set autocommit=0;')
        log_process(con, 'bf ins')
        print('bf ins')
        cr.executemany(sql, records_to_insert)
        log_process(con, 'af ins')
        print('af ins')
        cr.execute('commit;')
        cr.close()
    except Error as e:
        print(e)
        return False
    except Exception as e:
        error_log(con, 'data_mig->data_transfer_all->data_transfer_one->data_insert', e, datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))
        if con.is_connected():
            con.close()
            print('connection is closed')        
        return False    
    else:
        log_process(con, 'data_inserted..')
        print('data_inserted..')
        return True

def data_transfer_one(filename, dl_path):

    try:
        if not con.is_connected(): 
            raise Exception

        proc = dl_path[len(dl_path) - dl_path[::-1].find('\\', 2):-2][:2]

        #nxt_wday cur_wday home_path ls_data_path
        
        os.chdir(home_path+dl_path)
        records_to_insert = []
        log_process(con, 'inside transfer one for: ' + proc + ' : ' + home_path+dl_path+filename)
        print('inside transfer one for:', proc, ' : ', home_path+dl_path+filename)

        if proc == '01':                            #EQTY_PRICE_VOL 01
            with open(filename, 'r') as rfh: 
                reader = csv.reader(rfh)
                next(reader)
                for row in reader:
                    records_to_insert.append((row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12]))
                rfh.close()
            sql = "INSERT INTO st_eqty_price_vol (SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE, TOTTRDQTY, TOTTRDVAL, TIMESTAMP, TOTALTRADES, ISIN) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            data_insert(con, sql, records_to_insert)
        elif proc  == '02':                             #INDX_PRICE_VOL
            l_idx_open    = 0 
            l_idx_close    = 0 
            l_idx_high    = 0
            l_idx_low    = 0
            l_chnge        = 0
            l_p_change    = 0
            l_volume    = 0 
            l_turnover    = 0 
            l_PE        = 0
            l_PB        = 0 
            l_Yield        = 0
            
            with open(filename, 'r') as rfh:    
                reader = csv.reader(rfh)
                next(reader)
                for row in reader:
                    if row[2] == '-':
                        l_idx_open = 0
                    else:
                        l_idx_open = row[2]

                    if row[3] == '-':
                        l_idx_high = 0
                    else:
                        l_idx_high = row[3]

                    if row[4] == '-':
                        l_idx_low = 0
                    else:
                        l_idx_low = row[4]

                    if row[5] == '-':
                        l_idx_close = 0
                    else:
                        l_idx_close = row[5]

                    if row[6] == '-':
                        l_chnge = 0
                    else:
                        l_chnge = row[6]
                        
                    if row[7] == '-':
                        l_p_change = 0
                    else:
                        l_p_change = row[7]                        

                    if row[8] == '-':
                        l_volume = 0
                    else:
                        l_volume = row[8]
                        
                    if row[9] == '-':
                        l_turnover = 0
                    else:
                        l_turnover = row[9]
                        
                    if row[10] == '-' or row[10] == '':
                        l_PE = 0
                    else:
                        l_PE = row[10]
                        
                    if row[11] == '-' or row[11] =='':
                        l_PB = 0
                    else:
                        l_PB = row[11]                        

                    if row[12] == '-' or row[12] == 0:
                        l_Yield = 0
                    else:
                        l_Yield = row[12]                        

                    records_to_insert.append((row[0], datetime.strptime(row[1], '%d-%m-%Y').strftime('%Y-%m-%d'), l_idx_open, l_idx_high, l_idx_low, l_idx_close, l_chnge, l_p_change, l_volume, l_turnover, l_PE, l_PB, l_Yield))

                rfh.close()
            sql = "INSERT INTO st_indx_price_vol (indx,mk_date,open,high,low,close,chnge,p_change,volume,turnover,PE,PB,Yield) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            data_insert(con, sql, records_to_insert)
        elif proc  == '03':                             #M_EQTY  03 
            if filename.find('sec_list') > -1:                #security changed on 5th Jan 2021
                with open(filename, 'r') as rfh:
                    reader = csv.reader(rfh)
                    next(reader)
                    for row in reader:
                        if row[3] == 'No Band':
                            l_band = None
                        else:
                            l_band = row[3]
                        records_to_insert.append((row[0], row[1], row[2], l_band, row[4]))
                    rfh.close()
                sql = "INSERT INTO st_m_equities (Symbol, Series, Security_Name, Band, Remarks) VALUES (%s, %s, %s, %s, %s)"
                data_insert(con, sql, records_to_insert)                
            elif filename == 'series_change.csv':                #series change
                try:
                    with open(filename, 'r') as rfh:
                        reader = csv.reader(rfh)
                        next(reader)
                        for row in reader:
                            if len(row[5]) == 8:
                                records_to_insert.append(('SERIES', row[0], row[1], row[2], row[3], datetime.strptime(row[4], '%d-%b-%y').strftime('%Y-%m-%d'), row[5]))
                            elif len(row[5]) == 10:
                                records_to_insert.append(('SERIES', row[0], row[1], row[2], row[3], datetime.strptime(row[4], '%d-%b-%Y').strftime('%Y-%m-%d'), row[5]))
                        rfh.close()
                    sql = "INSERT INTO d_data (c1, c2, c3, c4, c5, c6, c7) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    data_insert(con, sql, records_to_insert)                
                except StopIteration as e:
                    print('no records')
            elif filename.find('eq_band_changes') > -1:            #equity price band change
                print('none......asdf')
                
                with open(filename, 'r') as rfh:
                    reader = csv.reader(rfh)
                    next(reader)
                    for row in reader:
                        if row[5] == 'No Band':
                            l_band = None
                        else:
                            l_band = row[5]                    
                        records_to_insert.append(('PRICEBAND', row[1], row[2], row[3], row[4], l_band))
                    rfh.close()
                sql = "INSERT INTO d_data (c1, c2, c3, c4, c5, c6) VALUES (%s, %s, %s, %s, %s, %s)"
                data_insert(con, sql, records_to_insert)                
                
        elif proc  == '04':                             #EQTY_DLVRY_POS 04 
            with open(filename, 'r') as rfh:
                for row in reversed(list(csv.reader(rfh))):
                    if row[0].upper().replace(' ', '') == 'RECORDTYPE':
                        break
                    records_to_insert.append((datetime.strptime(filename[4:-4], '%d%m%Y').strftime('%Y-%m-%d'), row[0], row[2], row[3], row[4], row[5], row[6]))
                rfh.close()
            sql = "INSERT INTO st_eqty_dlvry_pos (mk_date, record_type, symbol, series, qty_traded, dlvr_qty, dlvr_prcnt) VALUES (%s, %s, %s, %s, %s, %s, %s )"
            data_insert(con, sql, records_to_insert)                            
        elif proc  == '05':                             #VOLATILITY
            None
        elif proc  == '06':                             #M_INDICES
            None
    except Exception as e:
        error_log(con, 'data_mig->data_transfer_all->data_transfer_one', e, datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))
        if con.is_connected():
            con.close()
            print('connection is closed')        
        return False
    else:
        log_process(con, 'data_transfer_one complted for filename... ' + filename)
        print('data_transfer_one complted for filename... ', filename)
        return True

def data_transfer_all(con):

    try: 
        print('deleting previous day staging data..')
        cr = con.cursor()
        print('bf del')
        log_process(con, 'bf del')
        print('bf del')
        cr.execute('set autocommit=0;')
        cr.callproc('pr_del_prev_data')
        log_process(con, 'af del')
        print('af del')
        cr.execute('commit;')
        
        # print out the result
        for result in cr.stored_results():
            #log_process(con, result.fetchall())
                    print(result.fetchall())
        cr.close()        
        
        for eac in range(len(ls_data_path)):
            #dl_url, filename, dl_path
            
            filename = ls_data_path[eac][1]
            
            if ls_data_path[eac][1].find('.zip') > -1:
                filename = filename[:filename.find('.zip')]
            
            if not data_transfer_one(filename, ls_data_path[eac][2]):
                return False
        
        param = [datetime.strptime(nxt_wday,'%d-%b-%Y').strftime('%Y-%m-%d'), proc_id]
        print('inserting staging date into main tables', param[0], param[1])
        log_process(con, 'inserting staging date into main tables' + param[0] + ':' + str(param[1]))
        cr = con.cursor()
        log_process(con, 'before main tables..')
        print('before main tables..')
        cr.execute('set autocommit=0;')
        result_args = cr.callproc('pr_datatransfer', param)
        print(result_args)
        log_process(con, 'after main tables..')
        print('after main tables..')
        cr.execute('commit;')                
    except Error as e:
        error_log(con, 'data_mig->data_transfer_all', e, datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))
        if con.is_connected():
            con.close()
            print('connection is closed')        
        return False        
    except Exception as e:
        error_log(con, 'data_mig->data_transfer_all', e, datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))
        if con.is_connected():
            con.close()
            print('connection is closed')        
        return False
    else:
        log_process(con, 'data_transfer_all completed..')
        print('data_transfer_all completed..')
        return True
        
def build_url_dl(con):

    try:    
        print('setting global params...')
        global nxt_wday
        global cur_wday
        global home_path
        global ls_data_path
        global proc_id

        if not con.is_connected():
            print('db connection failed..')
            raise Exception

        print('collecting params..')
        
        sql = "select IFNULL(max(process_id), 0)+1 from process_log;"
        cr = con.cursor()
        cr.execute(sql)
        proc_id = cr.fetchone()[0]
        log_process(con, 'param process log :' + str(proc_id))
        print('param process log ', proc_id )
        
        sql = "select param_value from m_core_params where param = 'NEXT_TR_DT';"
        cr = con.cursor()
        cr.execute(sql)
        nxt_wday = cr.fetchone()[0]
        log_process(con, 'nxt_wday :' + nxt_wday)
        print('nxt_wday ', nxt_wday)

        # check if trading day.. otherwise return
        '''        
        if datetime.now().strftime('%d-%b-%Y').upper() != nxt_wday:
            log_process(con, 'not a trading day!!' + nxt_wday + ':' + datetime.now().strftime('%d-%b-%Y').upper())
            print('not a trading day!!',  nxt_wday + ':' + datetime.now().strftime('%d-%b-%Y').upper())
            return False
        '''    
        sql = "select param_value from m_core_params where param = 'CURR_TR_DT';"
        cr.execute(sql)
        cur_wday = cr.fetchone()[0]
        log_process(con, 'cur_wday :' + cur_wday)
        print('cur_wday ', cur_wday)
        
        sql = "select param_value from m_core_params where param = 'HOME_PATH';"
        cr.execute(sql)
        home_path = cr.fetchone()[0] 
        log_process(con, 'home_path :' + home_path)
        print('home_path ', home_path)

        sql = "select URL, filemask, DL_RLTV_PATH from md_url where md_freq = 'D' and active='Y';"
        cr.execute(sql)
        ls_data_path = cr.fetchall()
        ls_data_path = [list(i) for i in ls_data_path]
        
        print('Params are: nxt_dt:', nxt_wday, ' cur_dt:', cur_wday, ' home_path: ', home_path)
        print(' data_path :', ls_data_path)

        cr.close()
        
        if nxt_wday == '' or cur_wday == '' or home_path == '' or len(ls_data_path) ==0:
            print('build_url_dl 4')
            raise Exception
        
        log_process(con, 'building path..')
        print('building path..')

        for eac in range(len(ls_data_path)):
            #URL, filemask, DL_PATH
            
            dl_url = ls_data_path[eac][0]
            dl_url = dl_url.replace('DD', datetime.strptime(nxt_wday, '%d-%b-%Y').strftime('%d'))
            dl_url = dl_url.replace('MM' , datetime.strptime(nxt_wday, '%d-%b-%Y').strftime('%m'))
            dl_url = dl_url.replace('MON' , datetime.strptime(nxt_wday, '%d-%b-%Y').strftime('%b').upper())
            dl_url = dl_url.replace('YYYY' , datetime.strptime(nxt_wday, '%d-%b-%Y').strftime('%Y'))
            dl_url = dl_url.replace('YYYY'  , datetime.strptime(nxt_wday, '%d-%b-%Y').strftime('%Y'))
            ls_data_path[eac][0] = dl_url            
            
            filename = ls_data_path[eac][1]
            filename = filename .replace('DD', datetime.strptime(nxt_wday, '%d-%b-%Y').strftime('%d'))
            filename = filename .replace('MM' , datetime.strptime(nxt_wday, '%d-%b-%Y').strftime('%m'))
            filename = filename .replace('MON' , datetime.strptime(nxt_wday, '%d-%b-%Y').strftime('%b').upper())
            filename = filename .replace('YYYY' , datetime.strptime(nxt_wday, '%d-%b-%Y').strftime('%Y'))
            filename = filename .replace('YYYY'  ,  datetime.strptime(nxt_wday, '%d-%b-%Y').strftime('%Y'))
            ls_data_path[eac][1] = filename
            
            log_process(con, 'downloading.. ' + filename)
            print('downloading.. ', filename, ls_data_path[eac][0]+ls_data_path[eac][1])
            print(ls_data_path[eac][0]+ls_data_path[eac][1])
            urllib.request.urlretrieve(ls_data_path[eac][0]+ls_data_path[eac][1], home_path+ls_data_path[eac][2]+ls_data_path[eac][1])
            log_process(con, 'downloaded: ' + ls_data_path[eac][0]+ls_data_path[eac][1] + ' to- ' + home_path+ls_data_path[eac][2]+ls_data_path[eac][1])
            print('downloaded: ', ls_data_path[eac][0]+ls_data_path[eac][1], 'to-', home_path+ls_data_path[eac][2]+ls_data_path[eac][1])

            '''
            Sample url and path
            'https://www1.nseindia.com/content/historical/EQUITIES/YYYY/MON/', 'cmDDMONYYYYbhav.csv.zip', 'NSE\\DATA\\01_EQTY_PRICE_VOL\\');
            'https://archives.nseindia.com/content/indices/', 'ind_close_all_DDMMYYYY.csv', 'NSE\\DATA\\02_INDX_PRICE_VOL\\');
            'https://www1.nseindia.com/content/equties/', 'sec_list.csv', 'NSE\\DATA\03_M_EQTY\\');
            'https://www1.nseindia.com/content/equties/', 'series_change.csv', '\\NSE\\DATA\03_M_EQTY\\');
            'https://www1.nseindia.com/content/equities/', 'eq_band_changes_DDMMYYYY.csv', 'NSE\\DATA\\03_M_EQTY\\');
            'https://www1.nseindia.com/archives/equities/mto/', 'MTO_DDMMYYYY.DAT', 'NSE\\DATA\\04_EQTY_DLVRY_POS\\');            
            '''

    except HTTPError as e:
        if e.code == 404:
            print(e)        
            #error_log(con, 'data_mig->build_url_dl', e, datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))
            pass
        if con.is_connected():
            con.close()
            print('connection is closed')
        return False
    except Exception as e:
        print('Exception raised dd..', e)
        error_log(con, 'data_mig->build_url_dl', e, datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))
        if con.is_connected():
            con.close()
            print('connection is closed')
        return False
    else:
        log_process(con, 'build_url_dl completed..')
        print('build_url_dl completed..')
        return True

def data_mig():

    try:
        if not dbcon():
            print('Failed at dbcon..')
            raise Exception 
        if not build_url_dl(con): 
            print('Failed at build_url_dl..')
            raise Exception
        if not file_process('PRE'):
            print('failed at file_process..')
            raise Exception
        if not data_transfer_all(con):
            print('Failed at data_transfer..')
            raise Exception
        if not file_process('POST'):
            print('Failed at data_cleaning')
            raise Exception

    except Exception as e:
        if con.is_connected():
            con.close()
            print('connection is closed')
        print('Data migration failed..', e)
    else:
        log_process(con, 'Data migration completed..')
        print('Data migration completed..')
        if con.is_connected():
            log_process(con, 'connection is closed')
            print('connection is closed')
            con.close()
