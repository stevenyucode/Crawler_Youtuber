# 第一輪 "sb_youtube_info" : 先抓sb_country主頁的sb_rank + sb_url
# 第二輪 "youtube_info" : 依據抓到的sb_url, 連結到各國排名的網頁, 抓取channel_name, channel_type, type_rank, uploads, subs, video_views, channel_url
# 回到sb_country主頁, 抓取各國排名的type_rank, 再加回"youtube_info"
# 

import urllib.request
import pandas as pd
from bs4 import BeautifulSoup
from openpyxl import load_workbook
import sqlite3
import time
import requests
import threading
import queue
import os
import warnings
import re
import random
import pdb



# condition
countries = []
country_dict = {'us':'United States', 'de':'Germany', 'fr':'France', 'jp':'Japan', 'es':'Spain', 'gb':'Great Britain', 'au':'Australia', 'tw':'Taiwan', 'kr':'Korea South', 'it':'Italy'}

while True:
	country_input = input('Please enter the countries (or type q to exit) :')

	if country_input == 'q':
		break

	elif country_input in country_dict:
		countries.append(country_input)

	elif country_input not in country_dict:
		print('Country Not Found, please retry again !!')
	
rows = int(input('Please enter the number within the top ranks: '))

#1 pdb.set_trace()

# sb_youtube_info
sb_youtube_info = []

for country in countries:
	try:
		url = 'https://socialblade.com/youtube/top/country/' + str(country)
		headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
		time.sleep(1)
		response = requests.get(url, headers=headers)
		soup = BeautifulSoup(response.text, 'lxml')   # 使用lxml解析器, 回傳內容裡的文字, 也就是html內容傳入BeautifulSoup
		
		#2 pdb.set_trace()
	
	except BaseException as error:
		print('Expection Error:', error)
		print('Fail url:', url)

	else:
		table_soup = soup.find(style = re.compile('float: right; width'))
		row_soup = table_soup.find_all(style = re.compile('color:#444'))   # find_all的result為list -> []
		
		#3 pdb.set_trace()

		for row_data in row_soup:

			# row_soup這個list, 在當前迴圈row_data的index位置(index=0,1,2..) <= rows-1 (rows輸入2, 那就只取"前2次(名)" = index(0,1))
			if row_soup.index(row_data) <= rows-1:
				sb_rank_row = row_data.find(style = re.compile('color:#888'))

				# 只取排名數字, 去掉sb_rank後面的2個字母
				sb_rank_row_del_text = sb_rank_row.getText()[:-2]   # '1st' -> str本身就是list, 取index[:-2]=數字
				sb_rank = int(sb_rank_row_del_text)   # str -> int
							
				sb_url_row = row_data.find(href = re.compile('/youtube/user/')).attrs['href']
				sb_url = 'https://socialblade.com' + str(sb_url_row)

			else:
				break

			#4 pdb.set_trace()

			sb_link = [country, sb_rank, sb_url]
			sb_youtube_info.append(sb_link)

			#5 pdb.set_trace()

			print(time.strftime('%X', time.localtime()), country, 'rank', sb_rank, '& url : Done')
	print(time.strftime('%X', time.localtime()), country, ': Completed !!')

# to DataFrame
sb_youtube_info_df = pd.DataFrame(sb_youtube_info, columns=['country', 'sb_rank', 'sb_url'])
			
# to excel file
sb_youtube_info_df.to_csv('sb_youtube_info.csv', index=False, sep=',')

# # 連結/建立DB
# with sqlite3.connect('youtubers.db') as conn:
# 	sb_youtube_info_df.to_sql('sb_youtube_info', conn, index=False, if_exists='append')



# youtube_info
youtube_info = []

# # 開啟sqlite 連結
# with sqlite3.connect('youtubers.db') as conn:
# 	data = pd.read_sql('SELECT * FROM ' + 'sb_youtube_info', conn)   # sql: SELECT * FROM table_name 

# 讀取csv
sb_youtube_info_df = pd.read_csv('sb_youtube_info.csv')
	
# 讀出data的type為DataFrame => DataFrame需轉為List
sb_youtube_info = sb_youtube_info_df.values.tolist()

for sb_youtube_info_line in sb_youtube_info:
	country = sb_youtube_info_line[0]
	sb_rank = sb_youtube_info_line[1]
	url = sb_youtube_info_line[2]

	#6pdb.set_trace()

	try:
		headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
		time.sleep(1)
		response = requests.get(url, headers=headers)
		soup = BeautifulSoup(response.text, 'lxml')   # 使用lxml解析器, 回傳內容裡的文字, 也就是html內容傳入BeautifulSoup

	except BaseException as error:
		print('Expection Error:', error)
		print('Fail url:', url)

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

		#7pdb.set_trace()

		youtube_link = [country, sb_rank, channel_name, channel_type, type_rank, uploads, subs, video_views, channel_url]
		youtube_info.append(youtube_link)

		#8pdb.set_trace()
		
	print(time.strftime('%X', time.localtime()), country, 'rank', sb_rank, ': Info capture finished!!')
		
# to DataFrame
youtube_info_df = pd.DataFrame(youtube_info, columns=['country', 'sb_rank', 'channel_name', 'channel_type', 'type_rank', 'uploads', 'subs', 'video_views', 'channel_url'])
		
# to excel file
youtube_info_df.to_csv('youtube_info.csv', index=False, sep=',')

# # 連結/建立DB
# with sqlite3.connect('youtubers.db') as conn:
# 	youtube_info_df.to_sql('youtube_info', conn, index=False, if_exists='append')



# output excel file
with pd.ExcelWriter('final_excel.xlsx') as writer:
	
	youtube_info_df = pd.read_csv('youtube_info.csv')

	for country in countries:
		country_data = youtube_info_df['country'] == country  # 取出youtube檔案裡面column的country值, 等於當前迴圈使用的country值
		youtube_info_df[country_data].to_excel(writer, country, index=False)

		#9pdb.set_trace()

	# 丟棄不需要的 columns
	drop_column = ['sb_rank','channel_type', 'type_rank', 'uploads', 'subs', 'video_views']
	import_data = youtube_info_df.drop(drop_column, axis=1)
	
	# 加入新的columns
	import_data['Channel Type'] = 1
	import_data['Channel Sub Type'] = 2
	import_data['YouTube User Name'] = ''
	import_data['YouTube Playlist'] = ''
	import_data['Channel No'] = ''

	#10pdb.set_trace()

    #　重新排列 columns
	import_data.rename(columns = {'country':'Country', 'channel_name':'Channel Name', 'channel_url':'YouTube Channel ID'},inplace=True)
	col_order = ['Channel No', 'Country', 'Channel Type', 'Channel Sub Type', 'Channel Name', 'YouTube User Name', 'YouTube Channel ID', 'YouTube Playlist']
	import_data = import_data.reindex(columns = col_order)

	#11pdb.set_trace()

	# 利用import_data的index值, 搭配column來指定位置, 並變更數值
	for i in range(len(import_data)):
		import_data.loc[i, 'Channel No'] = i+1   # loc[i, Channel No]這個位置的值 = i+1
		import_data.loc[i, 'Country'] = country_dict[import_data.loc[i, 'Country']]
		import_data.loc[i, 'YouTube Channel ID'] = import_data.loc[i, 'YouTube Channel ID'].split('/')[-1]

		#12pdb.set_trace()

	import_data.to_excel(writer, sheet_name='import_data', index=False)


