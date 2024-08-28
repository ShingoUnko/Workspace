import pandas as pd # type: ignore
from datetime import date
import configparser
import Modules

#メイン関数
def main():
   #------------------------------------
   #S00 :初期化
   #------------------------------------
    args=[]
    df_opl_in=[]
    Df_new_Op=[]
    df_outDF=[]
    df_outDF=[]
    config_ini =()
    Sat=()
    Sats=[]
    Spn = ()
    Num = ()
    Opl_in = ()
    Aol_in = ()
    Can_out = ()
    data=()
    extracted_rows=[]
    df_combined=pd.DataFrame()
    df_Agp_combined=pd.DataFrame()
    flag=0
   #------------------------------------
   #S1000 :処理条件取得
   #------------------------------------
    # config.iniを取得する。
    config_ini = configparser.ConfigParser()
    config_ini.read('./Config.ini') #Config.iniの置き場所

    Sat=config_ini.get('CONDITION','sat')
    Sats=Sat.split(",")
    Spn = config_ini['CONDITION']['spn']
    Num = config_ini['CONDITION']['num']
    Ma  = config_ini['CONDITION']['ma']
    Opl_in = config_ini['CONDITION']['opl_in']
    Aol_in = config_ini['CONDITION']['aol_in']
    Can_out = config_ini['CONDITION']['can_out']
   #------------------------------------
   #S2000 :観測期間の取得
   ##  出力；１　Observation Listデータフレーム（df_opl_in）
   ##　　　：２　観測先頭日（Obs_st）
   ##　　　：３　観測期間（Obs_spn）
   #------------------------------------
    # 観測期間の取得  
    # #Opp List　読み込み
    df_opl_in = Modules.CsvToHdataflame(Opl_in,2)
    print("df_opl_in",df_opl_in)
#    print("df_opl_in",df_opl_in['observation_jst_time'])

    # "observation_jst_time"をdatatime型に変更 (ここまで)
    df_opl_in['observation_jst_time'] = pd.to_datetime(df_opl_in['observation_jst_time'])
#    print("df_opl_days",df_opl_in)

    # 観測開始日の取得
    Obs_st=Modules.GetObsST(df_opl_in)
#    print("観測開始日:",Obs_st)

    # 観測期間（リスト)の取得）
    Obs_spn=Modules.MakeObsSpan(Obs_st,int(Spn))
    print("観測期間:",Obs_spn)
   #------------------------------------
   #S3000 :"Aoi List"の前処理
   ##  出力；１　Aoi Listデータフレーム（df_aol_in）
   ##　　　：２　処理加工後の新しいAoi Listデータフレーム（Df_new_aol_sort）
   #------------------------------------
   #Observation List　読み込み
    df_aol_in = Modules.CsvTodataflame(Aol_in)
    print(df_aol_in)

   ##S3100 :前回の観測日取得新データフレームに追加
    #前回の観測記録からオフナディア角を抽出する
    Last_angle_list=Modules.GetAdd_LasObsAngleObsDt(df_aol_in)[0]
#    print(Last_angle_list)
    #前回の観測記録から観測日を抽出する
    Last_dt_list=Modules.GetAdd_LasObsAngleObsDt(df_aol_in)[1]
#    print(Last_dt_list)

   ##S3200 :新データフレームを作成し、リストを追加
    #新たなデータフレームを作成
    ##aoiName/Priority/Satellite/Angle(Min)/Angle(Max)/Orbit/Direction/Frequency
    Df_new_aol = df_aol_in[['AreaGroup','aoiName','Priority','Satellite','Angle(Min)','Angle(Max)','Orbit','Direction','Frequency','Margin']]
    #前回の観測記録をLast_Obsとして列を追加
    ## anlge
    Df_new_aol=Df_new_aol.assign(Last_angle=pd.to_numeric(Last_angle_list))
#    以下は、警告エラー処置後コメントアウト
#    Df_new_aol['Last_angle'] = Last_angle_list
#    Df_new_aol['Last_angle'] = pd.to_numeric(Df_new_aol['Last_angle'])

    ## date time
    Df_new_aol=Df_new_aol.assign(Last_dt=pd.to_datetime(Last_dt_list))    
#    以下は、警告エラー処置後コメントアウト
#    Df_new_aol['Last_dt'] = Last_dt_list
#    Df_new_aol['Last_dt'] = pd.to_datetime(Df_new_aol['Last_dt'])

    ## date 
    Df_new_aol['Last_day'] = Df_new_aol['Last_dt'].dt.date
    print(Df_new_aol)

   #------------------------------------
   #S4000 :AreaGroup毎にAOIリストをフィルタリング
   #------------------------------------
   ##4100 AreaGroup毎のリストを取得する。
    Agp_list=Modules.GetAgpList(Df_new_aol)

   ##S4200 :AreaGroupリスとに沿いAOIリストをフィルタリング
   # Agp_list毎にフィルタリング
    for AgNum in Agp_list:
      Df_filter_aol=Df_new_aol[Df_new_aol['AreaGroup']==AgNum]
      print("AreaGrup:",AgNum)

      #優先度順'Priority'にソート
      Df_new_aol_sort = Df_filter_aol.sort_values(by='Priority')
      print("Priorityにsort")
      print(Df_new_aol_sort)

      #データフレーム初期化
      df_combined=pd.DataFrame()
   #------------------------------------
   #S5000 :条件にあったAOIの観測候補を抽出する。
   ##  処理順位；以下を優先度上位から順に対応する。
   ##　　　　　：１　aoiName
   ##　　　　　：２　satellite
   ##　　　　　：３　flight_direction
   ##　　　　　：４　Direction
   ##　　　　　：５　Angle（最大、最小）
   ##　　　　　：６　Frequency
   #------------------------------------
   # データフレームを1行ごとに読み込む
      for index, row in Df_new_aol_sort.iterrows():

   ## 初期化
         flag=0 #flagを初期化
         filter_1=pd.DataFrame()
         filter_12=pd.DataFrame()
         filter_123=pd.DataFrame()
         filter_1234=pd.DataFrame()
         filter_12345=pd.DataFrame()
         filter_123456=pd.DataFrame()

   ## フィルタリング
   ##   条件１：aoi name
         print(f"aoiName:{row['aoiName']}")
#         filter_1=df_opl_in[df_opl_in['aoiName']=="aaa"] # type: ignore
         filter_1=df_opl_in[df_opl_in['aoiName']==row['aoiName']] # type: ignore

         if filter_1.empty:
            #エラー処理
           print("[メッセージ]aoiName:",row['aoiName'],"で絞り込んだ結果候補は0でした。")
         else:
           print(f"filter_1:{filter_1}")

   ##   条件２:Satellite
      #リストに変換
         filter_names=row['Satellite'].split(",")
      #フィルタリング
         filter_12=filter_1[filter_1['Satname'].isin(filter_names)]

         if filter_12.empty:
            #エラー処理
            print("[メッセージ]Satellite:",row['Satellite'],"で絞り込んだ結果候補は0でした。")
         else:
            print(f"filter_12:{filter_12}")
            
   ##   条件３:flight_direction
         if row['Orbit'] !="Ascending/Descending":
            filter_123=filter_12[filter_12['flight_direction']==row['Orbit']]
         else:
            filter_123=filter_12

         if filter_123.empty:
            #エラー処理
            print("[メッセージ]Orbit:",row['Orbit'],"で絞り込んだ結果候補は0でした。")
         else:
            print(f"filter_123:{filter_123}")
       
   ##   条件４: looking_direction
         if row['Direction'] !="Right/Left":
            filter_1234=filter_123[filter_123['looking_direction']==row['Direction']]
         else:
            filter_1234=filter_123

         if filter_1234.empty:
            #エラー処理
           print("[メッセージ]Direction:",row['Direction'],"で絞り込んだ結果候補は0でした。")
         else:
           print(f"filter_1234:{filter_1234}")

  ##   条件５: Angle（最大、最小）
      #最大値と最小値の範囲でフィルタリング
         filter_12345=filter_123[(filter_1234['offnadir_angl'] >= row['Angle(Min)']) & (filter_1234['offnadir_angl'] <= row['Angle(Max)']) ]

         if filter_12345.empty:
              print("[メッセージ]Angle(最小):",row['Angle(Min)'],"と","Angle（最大）",row['Angle(Max)'],"で絞り込んだ結果候補は0でした。")
         else:
           print(f"filter_12345:{filter_12345}")

  ##   条件６: Frequency
         if filter_12345.empty:
           flag=-1  
         elif row['Frequency']==0:
               #条件５の結果を返す
             flag=1
         elif row['Frequency']==-1:
             print("Frequency=-1のため",row['aoiName'],"を検索候補対象から外します")
         else :
#            print("row['Frequency']:",row['Frequency'])
#            print("int(Spn):",int(Spn))

            #観測頻度より観測候補をフィルタリングする。
#            print("Md",row['Margin'])
            filter_123456=Modules.Filter_freq(filter_12345,int(row['Frequency']),Obs_st,int(Spn),row['Last_day'],int(row['Margin']),row['Last_angle'],Ma) 
            # 観測頻度フィルタリングの結果候補が０の場合
            if filter_123456.empty :
               #条件５の結果を返す
               flag=1
            else:
               print("filter_123456",filter_123456)

  ##   flag=1の時、前回の観測値と一番近い候補を選出
         if flag == 1 : 
            print("[メッセージ]Angle(最小):",row['Angle(Min)'],"と","Angle（最大）",row['Angle(Max)'],"で絞り込んだ候補から最終候補を絞ります。")
            filter_123456=Modules.Filter_minAngle(filter_12345,row['Last_angle'],float(Ma))
            if filter_123456.empty:
               print("[メッセージ]前回の観測時オフナディア角:",row['Last_angle'],"との差がマージン:",float(Ma),"以上のため候補は0でした。")
               flag=-1
            else:
              print("filter_123456:",filter_123456) 

  ##   出力用データフレームに結合する
         if flag >-1:
           df_combined = pd.concat([df_combined,filter_123456],ignore_index=True)
           print("観測候補:",df_combined)
         else:
           print("[メッセージ]aoiName:",row['aoiName'],"の観測候補は0でした。")
            
   #------------------------------------
   #S5000 :csv出力用のデータフレーム作成
   #       抽出した候補情報から候補に絞ったデータフレームを作成
   ##  入力：１　観測候補
   ##  出力：２　csv出力用のデータフレーム
   #------------------------------------
   # 候補が１つもない場合
      if df_combined.empty:
         print("[メッセージ]AreaGrup:",AgNum,"からは観測候補は1つも選ばれませんでした。")
      else:

   # 対象衛星から、データフレームのcolumnsを作成する。
#         print("Num:",Num)
         sat_columns=Modules.Make_Sat_Colums(Sats,int(Num))
#         print(sat_columns)
   # 観測候補を加工し、Makeoutdataflameへ入力できる形にする。
         modify_df=Modules.Modify_DF(df_combined)
#         print("modify_df:",modify_df)
         df_Agp_results = Modules.Makeoutdataflame(df_combined,sat_columns,Obs_spn)
   #  AreaGroup　Noを追加する。
         df_Agp_results.insert(0,'AreaGroup',AgNum)
#         print("df_Agp_results:",df_Agp_results)
   #------------------------------------
   #S6000 :csv出力用のデータフレーム作成
   #       抽出した候補情報から候補に絞ったデータフレームを作成
   ##  入力：１　観測候補
   ##  出力：２　csv出力用のデータフレーム
   #------------------------------------
     ##   出力用データフレームに結合する
         df_Agp_combined = pd.concat([df_Agp_combined,df_Agp_results])
         print("AreaGroup:",AgNum)
         print("観測候補:",df_Agp_combined)

   #------------------------------------
   #S7000 :csvへ出力
   #------------------------------------
    if df_Agp_combined.empty:
      print("[メッセージ]""観測候補は0のため、csvファイルを出力しません。")
    else:
      Modules.dataflameToCsv(df_Agp_combined,Can_out)

#実行
if __name__ == "__main__":
  try:
   print("[メッセージ]""処理開始")
   main()
  except FileNotFoundError as e:
   print(f"エラーメッセージ:ファイルが見つかりません。:{e}")
   raise  
  except UnicodeDecodeError as e:
   print(f"エラーメッセージ:csvファイルの読み込み中にエラーが発生しました。:{e}")
   raise
  except KeyError as e:
   print(f"エラーメッセージ:入力ファイルの何処かの値に異常がありました。")
     
  else:
   print("[メッセージ]""処理終了")

