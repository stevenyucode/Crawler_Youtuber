'''
1. 第一輪 "sb_youtube_info" : 先抓sb_country主頁的sb_rank + sb_url
2. 使用抓取的sb_country + sb_rank + sb_url的 DataFrame 資料, 進行第二輪抓取.
3. 第二輪 "youtube_info" : 依據抓到的sb_url, 連結到各國排名的網頁, 抓取channel_name, channel_type, type_rank, uploads, subs, video_views, channel_url
4. 需回到sb_country主頁, 才能抓取各國排名的type_rank, 再加回"youtube_info".
5. 創建一個excel file, 導入youtube_info的資料, 並加以清洗整理.
6. 將所有國家資訊重新清洗成一個新的 "sheet", 並加入到excel file中. 

Note:
(1) Beautiful soup: find => 'str', find_all => [], select => []
'''

import pandas as pd
from bs4 import BeautifulSoup
from openpyxl import load_workbook
import sqlite3
import time
import requests
import threading
import os
import re
import random
import pdb


# 先取得sb_country, sb_rank, sb_url
def sb_youtube_info(countries, rows, country_dict):
	sb_youtube_info_list = []
	for country in countries:
		url = 'https://socialblade.com/youtube/top/country/' + str(country)
		soup = get_soup(url)
		table_soup = soup.find(style=re.compile('float: right; width'))
		row_soup = table_soup.find_all(style=re.compile('color:#444'))  # find_all的result為list -> []
		
		for row_data in row_soup:
			# row_soup這個list, 在當前迴圈row_data的index位置(index=0,1,2..) => 假如rows輸入2, 那就只取"前2次(名)" => 取index(0), index(1)
			if row_soup.index(row_data) < rows:
				sb_rank_row = row_data.find(style=re.compile('color:#888'))

				# 只取排名數字, 去掉sb_rank後面的2個字母
				sb_rank_row_del_text = sb_rank_row.text[:-2]   # '1st' -> 因為str本身就是list, 取index[:-2]=只剩數字
				sb_rank = int(sb_rank_row_del_text)   # str -> int
				# 取得url網址			
				sb_url_row = row_data.find(href=re.compile('/youtube/user/')).attrs['href']
				sb_url = 'https://socialblade.com' + str(sb_url_row)
			else:
				break

			sb_link = [country, sb_rank, sb_url]
			sb_youtube_info_list.append(sb_link)
		print(time.strftime('%X', time.localtime()), country_dict[country], 'Rank', sb_rank, 'Cralwer Completed !!')
	
	# to DataFrame
	sb_youtube_info_df = pd.DataFrame(sb_youtube_info_list, columns=['country', 'sb_rank', 'sb_url'])
	return sb_youtube_info_df


# 檢查取得bs
def get_soup(url):
	try:
		headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
		lock.acquire()
		time.sleep(2)   # 每個requests間隔2秒
		lock.release()
		response = requests.get(url, headers=headers)
		soup = response_Check(response, url)
	except BaseException as error:
		print('Expection Error:', error)
		time.sleep(random.randint(5, 8))  # 異常, 拉長抓取間隔時間
		soup = 0	
	return soup


# 檢查response[200] + 製作bs
def response_Check(response, url):
	if response.status_code == requests.codes.ok:   # code.ok = response[200]
		soup = BeautifulSoup(response.text, 'lxml')   # 使用lxml解析器, 回傳內容裡的文字, 也就是html內容傳入BeautifulSoup		
	else:
		print(response)
		print('Fail url:', url)
		time.sleep(random.randint(5, 8))  # 被阻擋, 拉長抓取間隔時間
		soup = 0
	return soup


# 使用多線程(10)取得youtube_info
def get_youtube_info(sb_youtube_info_line, youtube_info_list):
	with threading.Semaphore(10):   # 設定最多同時跑10個線程
		country = sb_youtube_info_line[0]
		sb_rank = sb_youtube_info_line[1]
		url = sb_youtube_info_line[2]
		soup = get_soup(url)
		# select出來的result為[], 所以result需表示為取出index[0]的值, 下一步才可使用此值
		channel_url = soup.select('#YouTubeUserTopSocial > div > .-margin')[0].attrs['href']  # attrs[]指定屬性獲取
		# 先縮小範圍為top_soup, 再從中取出需要的值
		top_soup = soup.select('#YouTubeUserTopInfoBlockTop')[0]
		# select的result, 需取出list值, 再使用取出Text
		channel_name = top_soup.select('div > h1')[0].text
		channel_type = top_soup.select('#youtube-user-page-channeltype')[0].text
		uploads = top_soup.select('#youtube-stats-header-uploads')[0].text
		subs = top_soup.select('#youtube-stats-header-subs')[0].text
		video_views = top_soup.select('#youtube-stats-header-views')[0].text
		table_soup = soup.find(style = re.compile('float: right; width'))
		type_rank = table_soup.select('div:nth-child(5) > div:nth-child(1) > p')[0].text
		youtube_link = [country, sb_rank, channel_name, channel_type, type_rank, uploads, subs, video_views, channel_url]
		youtube_info_list.append(youtube_link)
		# to DataFrame
		youtube_info_df = pd.DataFrame(youtube_info_list, columns=['country', 'sb_rank', 'channel_name', 'channel_type', 'type_rank', 'uploads', 'subs', 'video_views', 'channel_url'])
		youtube_info_df = youtube_info_df.sort_values(by=['country', 'sb_rank'], ascending=True)
		# 每條Thread完成時間不同, 故印出時間可能不會依照順序 
		print(time.strftime('%X', time.localtime()), country, 'rank:', sb_rank, '-> %s is Done...' % threading.current_thread().name)
		
		# CSV/DB切換
		# toCSV(youtube_info_df, 'youtube_info.csv')
		toDB(youtube_info_df, db_name='youtube.db', table_name='youtube_info')


# 開始收集youtube所有資料
def youtube_info():
	tasks = []
	youtube_info_list = []

	# CSV/DB切換
	# sb_youtube_info_df = readCSV('sb_youtube_info.csv')
	sb_youtube_info_df = readDB(db_name='youtube.db', table_name='sb_youtube_info')

	# 讀出data的type為DataFrame => DataFrame需轉為List
	sb_youtube_info_list = sb_youtube_info_df.values.tolist()

	for sb_youtube_info_line in sb_youtube_info_list:
		task = threading.Thread(target=get_youtube_info, args=(sb_youtube_info_line, youtube_info_list))  # args需用固定格式(x,y)		
		# 線程開始run
		task.start()
		# 將所有線程加到list內
		tasks.append(task)
	print(time.strftime('%X', time.localtime()), threading.active_count(), 'Thread Is Starting Crawlerd !!')
	
	counter = 0
	for task in tasks:
		counter += 1
		# 每個線程加入join, 確認跑完才會執行下一步
		task.join()
	# 確認所有線程已完成
	print(time.strftime('%X', time.localtime()), 'Total', counter, 'Thread Has Been Cralwer!!')


def toCSV(df, filename):
	lock.acquire()   # 到此之前方所有程式結束後, 才會繼續執行下一步
	df.to_csv(filename, index=False, sep=',')
	lock.release()


def readCSV(filename):
	# 檢查檔案在不在
	if os.path.isfile(filename):
		# 讀取csv => 轉成DataFrame
		df = pd.read_csv(filename)
	else:
		print(filename, 'is NOT found!!')
	return df


def toDB(df, db_name, table_name):
	# 連結/建立DB
	lock.acquire()   # 到此之前方所有程式結束後, 才會繼續執行下一步
	with sqlite3.connect(db_name) as conn:
		df.to_sql(table_name, conn, index=False, if_exists='replace')
	lock.release()


def readDB(db_name, table_name):
	# 連結DB
	with sqlite3.connect(db_name) as conn:
		df = pd.read_sql('select * from '+table_name, conn )
	return df


# 匯出成Excel檔案
def export_Excel(countries, country_dict):
	with pd.ExcelWriter('Youtube_Cralwer_Report.xlsx') as writer:
        
		# CSV/DB切換
		# youtube_info_df = readCSV('youtube_info.csv')
		youtube_info_df = readDB(db_name='youtube.db', table_name='youtube_info')

		for country in countries:
			# 取出youtube_info_df裡面 columns 的 country 值, 等於當前迴圈使用的country輸入值
			country_data = youtube_info_df['country'] == country
			# to EXCEL file
			youtube_info_df[country_data].to_excel(writer, country, index=False)

		# 丟棄不需要的 columns
		drop_column = ['sb_rank','channel_type', 'type_rank', 'uploads', 'subs', 'video_views']
		import_data = youtube_info_df.drop(drop_column, axis=1)

		# 加入新的columns
		import_data['Channel Type'] = 1
		import_data['Channel Sub Type'] = 2
		import_data['YouTube User Name'] = ''
		import_data['YouTube Playlist'] = ''
		import_data['Channel No'] = ''

	    #　重新排列 columns
		import_data.rename(columns={'country':'Country', 'channel_name':'Channel Name', 'channel_url':'YouTube Channel ID'}, inplace=True)
		col_order = ['Channel No', 'Country', 'Channel Type', 'Channel Sub Type', 'Channel Name', 'YouTube User Name', 'YouTube Channel ID', 'YouTube Playlist']
		import_data = import_data.reindex(columns=col_order)

		# 利用import_data的 index 與 column 位置來指定欄位 => loc(row, column), 並變更該欄位的數值
		for i in range(len(import_data)):
			import_data.loc[i, 'Channel No'] = i+1   # loc[i, Channel No]這個位置的值 = i+1 (原本是0開始)
			import_data.loc[i, 'Country'] = country_dict[import_data.loc[i, 'Country']]   # 變更為國家全名
			import_data.loc[i, 'YouTube Channel ID'] = import_data.loc[i, 'YouTube Channel ID'].split('/')[-1]  # 只取網址後段
		
		# Add new sheet to EXCEL file
		import_data.to_excel(writer, sheet_name='import_data', index=False)
		print(time.strftime('%X', time.localtime()), 'All Data has been import to Excel file')


def main():
	'''對話框輸入
	while True:
		country_input = input('Please enter the countries (type q to exit) :')
		if country_input == 'q':
			break
		elif country_input in country_dict:
			countries.append(country_input)
			countries.sort()
		elif country_input not in country_dict:
			print('Country Not Found, please try again!!')
	while True:
		rows = input('Please enter the ranking to be searched: ')
		if rows.isdigit():
			rows = int(rows)
			break
		else:
			print('Please enter digital !!')
			continue
	'''
	countries = ['us', 'fr', 'jp', 'es', 'tw'] 
	#['us', 'de', 'fr', 'jp', 'es', 'gb', 'au', 'tw', 'kr', 'it']
	country_dict = {'us':'United States', 'de':'Germany', 'fr':'France', 'jp':'Japan', 'es':'Spain', \
	'gb':'Great Britain', 'au':'Australia', 'tw':'Taiwan', 'kr':'Korea South', 'it':'Italy'}
	rows = 20
	sb_youtube_info_df = sb_youtube_info(countries, rows, country_dict)

	# CSV/DB切換
	# toCSV(sb_youtube_info_df, 'sb_youtube_info.csv')
	toDB(sb_youtube_info_df, db_name='youtube.db', table_name='sb_youtube_info')

	youtube_info()
	export_Excel(countries, country_dict)


# 啟動點
lock = threading.Lock()
main()
