# MSChallenger
挑戰者人口普查小工具

(以下內容全使用CHATGPT撰寫，程式本身也是使用CHATGPT完成)

📘 MSChallenger

MSChallenger 是一套基於 Nexon Open API 的 MapleStory 資料蒐集與分析工具，
提供角色、裝備、潛能、戰鬥力等資料的 自動化抓取、統計與視覺化介面。

📦 專案結構簡介
```
├─ app.py                    # Streamlit 主介面
├─ services.py               # 資料抓取 / best 裝備邏輯
├─ queries.py                # DB-only 查詢與統計
├─ tools_light_daemon.py     # 日常增量更新
├─ tools_basic_backfill.py   # 大量補資料
├─ tools_vacuum.py           # SQLite 最佳化
├─ tools_vacuum_into.py      
├─ build_env.py              # 環境建置（第一次用）
├─ requirements.txt
├─ apikey.txt                # 讀取apikey用 (如果沒有貼在裡面的話則每次重新啟動程式都需要重新輸入apikey)
└─ start.bat                 # 啟動主程式
└─ start_*.bat               # 各類工具快捷啟動
```


🚀 快速開始（Windows）

1️⃣ 建立環境（只需一次）
```
python build_env.py
```
2️⃣ 啟動主程式
```
start.bat
```
3️⃣ 日常更新basic資料(可單獨使用，不需要啟動主程式。主要抓取戰鬥力、等級等，預設每5分鐘打一次api，詳細參數可從tools_light_daemon.py調整)
```
start_tools_light_daemon.bat
```
4️⃣ 壓縮DB資料庫（選用，預設新的db檔案會儲存在D:，需自行搬移回來，詳細參數可從tools_vacuum_into.py調整）
```
start_vacuum.bat
```
(需合併使用)

5️⃣ 補齊缺漏資料(如性別、戰鬥力等過渡期間沒撈取到的資料，少用)
```
start_tools_basic_backfill(補資料).bat
```
6️⃣ 刪除過期角色資料(如更改ID、找不到資料、被停權等，程式設計連續5次撈不到角色資料即移除該筆數據)
```
run_basic_backfill_delete_after_5(刪資料).bat
```

# 第一次使用
- 需自行申請NEXON API KEY，並貼上到apikey.txt (測試版的會有次數限制)
![image](https://github.com/bac925/MSChallenger/blob/main/readme_img/readme_01.jpg?raw=true)

## 公會清單
- 啟動網頁後，需自行整理並輸入公會清單(可自行從榮耀之石截圖並使用OCR工具轉文字)

- 輸入完清單後，可以到第二步進行角色清單抓取，抓取完之後程式會提示有沒有公會名稱輸入錯誤，有的話可以再回到第一步下方修正錯誤的公會名稱並儲存，然後再抓取一次角色清單
![image](https://github.com/bac925/MSChallenger/blob/main/readme_img/readme_02.jpg)
![image](https://github.com/bac925/MSChallenger/blob/main/readme_img/readme_03.jpg)

## 角色清單相關功能
- 第二步下方提供角色清單健檢的功能
  
  - Step2.5 可篩選抓不到ocid/basic(無法用角色ID從API取得資料，例如改名)、access_flag=false(七天內未登入的非活躍帳號)並移除
  - Step2.6 可從遊戲官網自動撈取系統自動停權的名單後從資料庫刪除
    
![image](https://github.com/bac925/MSChallengerr/blob/main/readme_img/readme_04.png)

- 從左方調整要抓取的資料區間跟參數後，點擊開始抓取，即會從API撈取資料。
![image](https://github.com/bac925/MSChallenger/blob/main/readme_img/readme_05.png)

### ※stat等與戰鬥力相關的數據若使用歷史數據可能會造成失準，建議第一次跑完資料之後繼續用start_tools_light_daemon.bat做長時間的即時數據追蹤，等級跟戰鬥力會抓得比較準確。

## 數據統計相關功能
![image](https://github.com/bac925/MSChallenger/blob/main/readme_img/readme_06.png)

- 數據統計專區提供以下功能
  - 等級人數、戰鬥力人數區間分佈
  - 等級&戰鬥力TOP20
  - 角色性別比例(也有獨立做一個蓮的)
  - 職業比例
  - 統計解放創世武器人數
  - ID查表功能(查單一角色、查ID名稱)
  - 高級菇菇通行證神秘武器人數統計
  - 輪迴碑石、滿捲的挑戰者圖騰(40000分)持有人數統計
  - 持有指定裝備潛能詞條的人數(武器、副武、三武
  
如想要在統計資料中排除分身角色，也可以從上面排除指定等級以下的角色。

## 由於後續應該不會再做更新，可以自行拿去做額外修改或研究，不需要額外詢問。
