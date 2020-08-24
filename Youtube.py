# 第一輪 "sb_youtube_info" : 先抓sb_country主頁的sb_rank + sb_url
# 第二輪 "youtube_info" : 依據抓到的sb_url, 連結到各國排名的網頁, 抓取channel_name, channel_type, type_rank, uploads, subs, video_views, channel_url
# 回到sb_country主頁, 抓取各國排名的type_rank, 再加回"youtube_info"
# Beautiful soup: find -> 'str', find_all -> [], select -> []

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



# condition
countries = ['us', 'fr', 'jp', 'es', 'gb'] #,de ,au , tw, kr, it]
country_dict = {'us':'United States', 'de':'Germany', 'fr':'France', 'jp':'Japan', 'es':'Spain', 'gb':'Great Britain'} #'au':'Australia', 'tw':'Taiwan', 'kr':'Korea South', 'it':'Italy'}
rows = 5

# while True:
# 	country_input = input('Please enter the countries (type q to exit) :')

# 	if country_input == 'q':
# 		break
# 	elif country_input in country_dict:
# 		countries.append(country_input)
# 		countries.sort()
# 	elif country_input not in country_dict:
# 		print('Country Not Found, please try again!!')

# while True:
# 	rows = input('Please enter the ranking to be searched: ')

# 	if rows.isdigit():
# 		rows = int(rows)
# 		break
# 	else:
# 		print('Please enter digital !!')
# 		continue



# sb_youtube_info
sb_youtube_info = []
lock = threading.Lock()
headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}

for country in countries:
	try:
		url = 'https://socialblade.com/youtube/top/country/' + str(country)
		lock.acquire()
		time.sleep(3)   # 每個requests間隔3秒
		lock.release()
		response = requests.get(url, headers=headers)

		if response.status_code == requests.codes.ok:   # code.ok = response[200]
			soup = BeautifulSoup(response.text, 'lxml')   # 使用lxml解析器, 回傳內容裡的文字, 也就是html內容傳入BeautifulSoup
		
		else:
			print(response)
			print('Fail url:', url)
			time.sleep(random.randint(5, 10))
			soup = 0
	
	except BaseException as error:
		print('Expection Error:', error)
		soup = 0

	else:
		table_soup = soup.find(style = re.compile('float: right; width'))
		row_soup = table_soup.find_all(style = re.compile('color:#444'))   # find_all的result為list -> []

		for row_data in row_soup:

			# row_soup這個list, 在當前迴圈row_data的index位置(index=0,1,2..) <= rows-1 (假如rows輸入2, 那就只取"前2次(名)" => 取index(0), index(1))
			if row_soup.index(row_data) <= rows-1:
				sb_rank_row = row_data.find(style = re.compile('color:#888'))

				# 只取排名數字, 去掉sb_rank後面的2個字母
				sb_rank_row_del_text = sb_rank_row.getText()[:-2]   # '1st' -> 因為str本身就是list, 取index[:-2]=只剩數字
				sb_rank = int(sb_rank_row_del_text)   # str -> int
								
				sb_url_row = row_data.find(href = re.compile('/youtube/user/')).attrs['href']
				sb_url = 'https://socialblade.com' + str(sb_url_row)
			
			else:
				break

			sb_link = [country, sb_rank, sb_url]
			sb_youtube_info.append(sb_link)

		# print(time.strftime('%X', time.localtime()), country, 'rank', sb_rank, '& url : Done')
	print(time.strftime('%X', time.localtime()), country_dict[country], 'Rank', sb_rank, 'Cralwer Completed !!')

# to DataFrame
sb_youtube_info_df = pd.DataFrame(sb_youtube_info, columns=['country', 'sb_rank', 'sb_url'])
			
# to excel file
sb_youtube_info_df.to_csv('sb_youtube_info.csv', index=False, sep=',')



def get_youtube_info(sb_youtube_info_line):
	
	with threading.Semaphore(10):   # 設定最多同時跑10個線程
		country = sb_youtube_info_line[0]
		sb_rank = sb_youtube_info_line[1]
		url = sb_youtube_info_line[2]

		try:
			lock.acquire()
			time.sleep(3)  # 每個request間隔3秒
			lock.release()
			response = requests.get(url, headers=headers)
			
			if response.status_code == requests.codes.ok:
				soup = BeautifulSoup(response.text, 'lxml')   # 使用lxml解析器, 回傳內容裡的文字, 也就是html內容傳入BeautifulSoup
			
			else:
				print(response)
				print('Fail url:', url)
				time.sleep(random.randint(5, 10))
				soup = 0

		except BaseException as error:
			print('Expection Error:', error)
			print('Fail url:', url)
			soup = 0

		else:
		# select出來的result為[], 所以result需表示為取出index[0]的值, 下一步才可使用此值
			channel_url = soup.select('#YouTubeUserTopSocial > div > .-margin')[0].attrs['href']  # attrs[]指定屬性獲取

			# 先縮小範圍為top_soup
			top_soup = soup.select('#YouTubeUserTopInfoBlockTop')[0]
				
			# 在top_soup內, 取出需要的值
			# select的result, 需取出list值, 再使用getText() => 只能用在純文字檔(str)
			channel_name = top_soup.select('div > h1')[0].getText()
			channel_type = top_soup.select('#youtube-user-page-channeltype')[0].getText()
			uploads = top_soup.select('#youtube-stats-header-uploads')[0].getText()
			subs = top_soup.select('#youtube-stats-header-subs')[0].getText()
			video_views = top_soup.select('#youtube-stats-header-views')[0].getText()

			table_soup = soup.find(style = re.compile('float: right; width'))
			type_rank = table_soup.select('div:nth-child(5) > div:nth-child(1) > p')[0].getText()

			youtube_link = [country, sb_rank, channel_name, channel_type, type_rank, uploads, subs, video_views, channel_url]
			youtube_info.append(youtube_link)

			# to DataFrame
			youtube_info_df = pd.DataFrame(youtube_info, columns=['country', 'sb_rank', 'channel_name', 'channel_type', 'type_rank', 'uploads', 'subs', 'video_views', 'channel_url'])
			# youtube_info_df = youtube_info_df.sort_values(['country', 'sb_rank'], ascending=True)

			# 每條Thread完成時間不同, 故印出時間可能不會依照順序 
			print(time.strftime('%X', time.localtime()), country, 'rank:', sb_rank, '-> %s is Done...' % threading.current_thread().name)

			# 等前方工作全部結束, 才會存到csv
			lock.acquire()

			# to excel file
			youtube_info_df.to_csv('youtube_info.csv', index=False, sep=',')

			lock.release()

			

# youtube_info
tasks = []
youtube_info = []

# 讀取csv => 轉成DataFrame
sb_youtube_info_df = pd.read_csv('sb_youtube_info.csv')
	
# 讀出data的type為DataFrame => DataFrame需轉為List
sb_youtube_info = sb_youtube_info_df.values.tolist()

for sb_youtube_info_line in sb_youtube_info:

	task = threading.Thread(target=get_youtube_info, args=(sb_youtube_info_line,))  # args需用固定格式(x,y)
	# print(threading.active_count())  # 查看目前有多少線程數 (task會先收集所有線程, 此處線程持續增加)
	
	# 線程開始run
	task.start()
	# 將所有線程加到list內
	tasks.append(task)

print(time.strftime('%X', time.localtime()),'Main +', threading.active_count()-1, 'Thread Is Starting Crawlerd !!')

counter = 0
for task in tasks:
	counter += 1
	task.join()   # 每個線程加入join, 確認跑完才會執行下一步
	
# 確認所有線程已完成
print(time.strftime('%X', time.localtime()), 'Total', counter, 'Thread Has Been Cralwer!!')



# output excel file
with pd.ExcelWriter('final_excel.xlsx') as writer:
	
	youtube_info_df = pd.read_csv('youtube_info.csv')

	for country in countries:
		country_data = youtube_info_df['country'] == country  # 取出youtube檔案裡面column的country值, 等於當前迴圈使用的country值(=keyin)
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
	import_data.rename(columns = {'country':'Country', 'channel_name':'Channel Name', 'channel_url':'YouTube Channel ID'},inplace=True)
	col_order = ['Channel No', 'Country', 'Channel Type', 'Channel Sub Type', 'Channel Name', 'YouTube User Name', 'YouTube Channel ID', 'YouTube Playlist']
	import_data = import_data.reindex(columns = col_order)

	# 利用import_data的index與column位置來指定欄位 -> loc(row, column), 並變更該欄位的數值
	for i in range(len(import_data)):
		import_data.loc[i, 'Channel No'] = i+1   # loc[i, Channel No]這個位置的值 = i+1 (原本是0開始)
		import_data.loc[i, 'Country'] = country_dict[import_data.loc[i, 'Country']]   # 變更為國家全名
		import_data.loc[i, 'YouTube Channel ID'] = import_data.loc[i, 'YouTube Channel ID'].split('/')[-1]  # 只取網址後段

	import_data.to_excel(writer, sheet_name='import_data', index=False)


