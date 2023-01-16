from import_libs import *

def get_data(start, end, ID, interval, enctoken):
  url="https://kite.zerodha.com/oms/instruments/historical/" + str(ID) + "/" + interval
  p = {
      "user_id" :"QZA242",
      "oi" : "1",
      "from" :start,
      "to": end,
      "user_id":"QZA242",
      "enctoken": enctoken
    }
  h= {"authorization":enctoken}

  p = requests.get(url, params=p,headers=h)

  data = p.content
  data = data.decode('utf-8')
  valid_json_string = "[" + data + "]"  # or "[{0}]".format(your_string)
  data = json.loads(valid_json_string)
  df = pd.DataFrame(data[0]['data']['candles'])

  return df

def get_csv(ec, fr, to, ID, SYMBOL, interval, path):

  enctoken = "enctoken {}".format(ec)

  st = datetime.strptime(fr, '%d-%m-%Y').date()
  en = datetime.strptime(to, '%d-%m-%Y').date()
  thedate = st
  bigdata = pd.DataFrame(data=None)
  while(thedate<=en):

    nextdate = thedate + timedelta(days=30)
    if(nextdate > en):
      nextdate = en

    #print(thedate)
    #print(nextdate)
    df = get_data(thedate.strftime('%Y-%m-%d'), nextdate.strftime('%Y-%m-%d'), ID, interval, enctoken)
    bigdata = pd.concat([bigdata,df])
    thedate =  thedate + timedelta(days=31)
  #print(bigdata)
  bigdata[0] = bigdata[0].apply(lambda x: x.replace("+0530"," "))
  bigdata[0] = bigdata[0].apply(lambda x: x.replace("T"," "))
  bigdata['Date'] = bigdata[0].apply(lambda x: x[0:11])
  bigdata['Time'] = bigdata[0].apply(lambda x: x[11:])
  data = pd.DataFrame(data={'Date':bigdata['Date'], 'Time':bigdata['Time'], 'open':bigdata[1], 'high':bigdata[2],
                            'low':bigdata[3], 'close':bigdata[4], 'volume':bigdata[5]})
  data = data.reset_index(drop=True)
  data.to_csv(path+SYMBOL+'_'+interval+'.csv')
