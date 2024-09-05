# Modules.py
# 独自関数の定義ファイル

import pandas as pd # type: ignore
from datetime import date
import configparser
import datetime

'''
---------------
01.CSVからデータフレームに入力する。
---------------
<入力>
  1:csvフィルパス
'''
def CsvTodataflame(dir):
    #初期化
    df=pd.DataFrame()
    df_datafrmae=pd.DataFrame()

    #CSVファイルをDataframeに入力
    df= pd.read_csv(dir,encoding="shift-jis")

    #読み込んだ文字列の末尾に空白があれば削除する
    df_datafrmae=df.applymap(lambda x:x.rstrip() if isinstance(x,str) else x)

    return df_datafrmae


'''
---------------
02.CSVからデータフレームに(ヘッダ指定で）入力する。
---------------
<入力>
  1:csvフィルパス
  2:基準となるヘッダ行数
<出力>
  1:データフレーム
'''
def CsvToHdataflame(dir,head_num):
  # csvから指定ヘッダ以下を抽出し、データフレームへ入力する。
    df = pd.read_csv(dir, header=head_num)
    return df

'''
---------------
03.観測開始日取得
---------------
<入力>
  1:データフレーム
  2:観測期間（span)
<出力>
  1:観測開始日（YYYY-MM-DD）
'''
def GetObsST(df):

  Obs_data_time='observation_jst_time'
  #データフレームを昇順にソートする。
  df_sorted=df.sort_values(by=Obs_data_time)
#  print ("df_sorted:",df_sorted)

  #ソートした値の先頭を取得する。
  obs_st_date=df_sorted[Obs_data_time].iloc[0]
#  print ("obs_st_date:",obs_st_date)

  #ソートした値の先頭の日付をを取得する。（先頭から10文字抽出）
  obs_st_day=obs_st_date.date()
#  print(obs_st_day)

  return obs_st_day

'''
---------------
04.観測候補出力期間を作成する
---------------
<入力>
  1:観測開始日（YYYY-MM-DD）
<出力>
  1:観測期間(list):range
'''
def MakeObsSpan(st_day,num):
  #ソートした値の先頭の日付を基準に、。（先頭から10文字抽出）
  #st_day='2024/12/1'
  range=pd.date_range(start=st_day,periods=num).strftime('%Y/%m/%d').tolist()
  return range

'''
---------------
05.AreaGroupのリストを取得する
---------------
<入力>
  1:データフレーム
<出力>
  1:AreaGroupリスト（ソート済み）
'''
def GetAgpList(df):
  # AreaGroup 情報取得
  agp_list=df['AreaGroup']
#  print("agp_list:",agp_list)

  #  AreaGroup 重複削除し、リスト化
  unique_data=sorted(agp_list.unique().tolist())
#  print("unique_data:",unique_data)

  return unique_data

'''
---------------
06.最終観測日のオフナディア角を抽出して、DatFramenに追加する。
---------------
<入力>
  1:データフレーム
<出力>
  1:オフナディア角［リスト形式］
  2:観測日[datetime型］[リスト形式]

'''
def GetAdd_LasObsAngleObsDt(df):
  #各行の最後の値を抽出
  last_obs_times=df.apply(lambda row:row[row.last_valid_index()],axis=1)
  last_obs_dt=[line.split('\n')[-1].split()[-1] for line in last_obs_times]
  last_obs_angle=[line.split('\n')[0].split()[-1] for line in last_obs_times]
#  print(last_obs_angle)
#  print(last_obs_dt)

  return(last_obs_angle, last_obs_dt)
'''
---------------
07. 観測頻度（日数）の条件にあわせてフィルタリングし該当データをデータフレームで返す
---------------
<入力>
  1:データフレーム
  2:観測頻度（日数）
  3:観測開始日
  4:観測期間（日数）
  5:マージン期間
  6:前回の観測時のオフナディア角
  7:オフナディア角マージン
<出力>
  1:抽出したリスト
'''
def Filter_freq(df,freq_days,st,span,t_day,m_days,last_angle,Ma):
  #初期化
  base_day=t_day
  observation_dates=[]
  df_filter_day=pd.DataFrame()
  df_filter_days=pd.DataFrame()
 
  #観測終了日の取得
  et=st+datetime.timedelta(days=span)
#  print("et",et)
#  print("base_day",base_day)
#  print("freq_days",freq_days)

  #観測候補日のリスト
  #while文の条件見直し
  while True:
     base_day= base_day + datetime.timedelta(days=freq_days)
     if base_day > et:
        break
     observation_dates.append(base_day)

#  print("observation_dates:",observation_dates)

  #[観測頻度] - マージン日数×2 を求める
#  jg_value=freq_days-m_days*2

  #観測候補日±マージンを加味してフィルタリングを実施する。
  #かつ、フィルタリング結果を前回の観測結果と比較し、最も近しい値を抽出する。
  for day in observation_dates:
#    print("m_days:",m_days)
    df_filter_day = df[(df['observation_jst_time'].dt.date>=day+datetime.timedelta(days=-m_days))&(df['observation_jst_time'].dt.date<=day+datetime.timedelta(days=m_days))]
#    print("day",day)
#    print("df_filter_day",df_filter_day)
    if len(df_filter_day) >1:
       df_filter_filter_day=Filter_minAngle(df_filter_day,last_angle,float(Ma))
    else:
       df_filter_filter_day=df_filter_day
#    print("df_filter_filter_day",df_filter_filter_day)
    df_filter_days = pd.concat([df_filter_days,df_filter_filter_day],ignore_index=True)

#  print("df_filter_days",df_filter_days)
  #重複データがある場合消す
  df_filter_days_del_dupl = df_filter_days.drop_duplicates()

  return df_filter_days_del_dupl

'''
---------------
08.前回の観測結果（オフナディア角）と比較し最も近しい観測候補を抽出する
---------------
<入力>
  1:データフレーム
  2:マージン角
<出力>
  1:１データフレーム
'''
def Filter_minAngle(df,angle,m_angle):
  #初期化
  df_filter=pd.DataFrame()
  #各行の最後の値を抽出
  df_filter=df.loc[[abs(df['offnadir_angl'] - angle).idxmin()]]
  
#  print("angle:",angle)
#  print("m_angle:",m_angle)
#  print("df_filter['offnadir_angl']:",df_filter['offnadir_angl'])
#  print("abs:",abs(df_filter['offnadir_angl'] - angle))

  if abs(df_filter['offnadir_angl'] - angle).iloc[0] >= m_angle:
     df_filter=pd.DataFrame()

  return(df_filter)

''''
---------------
09.CSV格納用情報作成
# Orbit/Direction
# angle
# Observation time
---------------
<入力>
  1:データフレーム
<出力>
  1:加工したデータフレーム

'''
def Modify_DF(df):
  #”インデックス名”の列に、Satnameとflight_directionを組み合わせた
  #文字列を格納する。
  df['index name'] = df['Satname']+" "+df['flight_direction'].str[0:3]

#出力する際の、"aoiName" "observation_jst_time" "offnadir_angl" "flight_direction""looking_direction"
#文字列を作成する。
# 条件
# ・"offnadir_angl"は、小数点１桁に丸めている。
# ・'observation_jst_time'は、文字列末尾より５文字の時刻（hh:mm）のみ出力する。
  frame1 = df['aoiName']+"\n"+df['flight_direction'].str[0]+df['looking_direction'].str[0]
  frame2 = df['offnadir_angl'].round(1).astype(str)
  frame3 = df['observation_jst_time'].dt.strftime("%H:%M:%S")
  df['obs_opp_list']=frame1+" "+frame2+"\n"+frame3
  return df

'''
---------------
10.出力用のcolumsを作成する。
---------------
<入力>
  1:衛星名のリスト
<出力>
  1:衛星名+ "orbit(Asc/Des)"
'''
#def Make_Sat_Colums(list):
def Make_Sat_Colums(list,num):
    new_list=[]

    for i in range(num):
#      print("num:", num)
      for item in list:
          prefix, number=item.split('-')
          new_list.append(prefix+"-"+number+" "+"Asc")
          new_list.append(prefix+"-"+number+" "+"Des")
    
    return(new_list)

'''
---------------
11.出力する型でデータフレームを作成する。
---------------
 csv出力するデータフレームを作成する。
<入力>
  1:フィルタリングの結果のフレームワーク
  2:観測候補出力期間
  3:観測衛星DES/ACEリスト
<出力>
  1:データフレーム
<処理方式>
  ①入力情報、①、②をpropertyファイルから取得する。
  ②①の情報から、出力するフレームワークのindexとcolumnsを定義する。
  ③②のフレームのindex及びcolumnsに合致する値=valueを抽出し
   新しいフレームワークにvalueとして格納する。
'''
def Makeoutdataflame(df,indexs,columns):
    #1.初期設定
    #1.1 初期化
    df_obsList  = ()
    df_opp_List = ()
    data = ()
    obs_date = ()
    sat_info = ()
    opp_info = ()
    indexs_sat = ()
    columns_day = ()

    #1.2 ファイル読み込み
    # input.csv(観測候補）をデータフレームに取り込む
    df_obsList = df
#    print (df_obsList)

  #  #1.3  observation_jst_time 日時データを日付データに変える
  #  df_obsList['observation_jst_time'] = pd.to_datetime(df_obsList['observation_jst_time']).dt.date
  #  print (df_obsList)

    #1.4 定義
    indexs_sat  = indexs
    columns_day = columns #datetime型に変更
    opp_info = "-" # 全て-を設定する
    df_opp_List = pd.DataFrame(data=opp_info,columns=columns_day,index=indexs_sat)
#    print(df_opp_List)

    # データフレームを1行ごとに読み込む
    for index, data in df_obsList.iterrows():

        # 'index name' と'obs_opp_list'と'observation_jst_time'を
        # 抜き出す
        obs_date = data['observation_jst_time'].strftime("%Y/%m/%d") 
        sat_info = data['index name']
        opp_info = data['obs_opp_list']
    
        #index No.
#        print("--------------------")
#        print("No.:",index+1)
        #sat info
#        print("obs_date:",obs_date)
#        print("sat_info:",sat_info)
#        print("opp_info",opp_info)

        #初期化
        i = 0
        l = 0
    
        #配列に格納する。
#        print("len(columns_day):",len(columns_day))
#        print("len(indexs_sat):",len(indexs_sat))    
        for i in range(len(columns_day)) :
#            print("i=",i)
            if columns_day[i] == str(obs_date) :
#                print("obs_date",obs_date)
                for l in  range(len(indexs_sat)):
#                    print("l=",l)
#                    print("sat_info",sat_info)
#                    print("df_opp_List.iloc[l,i]",df_opp_List.iloc[l,i])
                    if indexs_sat[l] == str(sat_info) and df_opp_List.iloc[l,i]=="-":
                        df_opp_List.iloc[l,i] = opp_info #df_opp_List.iloc[行番号,列番号]
#                        print ("opp_info",opp_info)
                        break
                    else:
                        continue
            else:
                continue

#    print(df_opp_List)
    return(df_opp_List)

'''
---------------
12.csv出力関数
---------------
<入力>
  ①データフレーム
  ②出力先
<出力>
  ①リターンコード 
'''

def dataflameToCsv(df,dir):
    #今日の日付を取得
#    today = date.today().strftime('%Y%m%d')
    #実行日時を取得
    now = datetime.datetime.now()
    today_now = now.strftime('D%Y%m%dT%H%M%S')

    #ファイル名に今日の日付を付与
    filename = f'Observation_Candidate_List_{today_now}.csv'

    #CSVファイルの出力
    df.to_csv(dir+filename)

