
# 第一輪抓sb_country sb_rank + sb_url
# 第二輪依據 sb_url, 進入連結抓取 channel_name, channel_type, type_rank, uploads, subs, video_views, channel_url
# 再加回 sb_country, sb_rank


import urllib.request
import pandas as pd
from bs4 import BeautifulSoup
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


countries = []
while True:
	country = input('Please enter the countries: ')

	if country == 'q' :
		break
	else:
		countries.append(country)
	
rows = int(input('Please enter the number within the top ranks: '))


# sb_youtube_info
sb_youtube_info = []

for country_select in countries:
	try:
		url = 'https://socialblade.com/youtube/top/country/' + str(country_select)
		headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
		time.sleep(1)
		response = requests.get(url, headers=headers)
		soup = BeautifulSoup(response.text, 'lxml')   # 使用lxml解析器, 回傳內容裡的文字, 也就是html內容傳入BeautifulSoup

	except BaseException as error:
		print('Expection Error:', error)

	else:
		table_soup = soup.find(style = re.compile('float: right; width'))
		row_soup = table_soup.find_all(style = re.compile('color:#444'))
		
		for row_data in row_soup:

			if row_soup.index(row_data) <= rows-1 :
				sb_rank_row = row_data.find(style = re.compile('color:#888'))

				# 去掉sb_rank後面的2個字母
				sb_rank_row_del_text = list(sb_rank_row.getText())[:-2]
				sb_rank = int(''.join(sb_rank_row_del_text))     # list -> str -> int
							
				sb_url_row = row_data.find(href = re.compile('/youtube/user/')).attrs['href']
				sb_url = 'https://socialblade.com' + str(sb_url_row)
						
			else:
				break

			sb_link = [country_select, sb_rank, sb_url]
			sb_youtube_info.append(sb_link)
			
# to DataFrame
sb_youtube_info_df = pd.DataFrame(sb_youtube_info, columns=['country_select', 'sb_rank', 'sb_url'])
			
# to excel file
sb_youtube_info_df.to_csv('sb_youtube_info.csv', index=False, sep=',')

# 連結/建立DB
with sqlite3.connect('youtubers.db') as conn:
	sb_youtube_info_df.to_sql('sb_youtube_info', conn, index=False, if_exists='append')



# youtube_info
youtube_info = []

# 開啟sqlite 連結
with sqlite3.connect('youtubers.db') as conn:
	data = pd.read_sql('SELECT * FROM ' + 'sb_youtube_info', conn)   # sql: SELECT * FROM table_name 
	
# 讀出data的type為DataFrame => DataFrame需轉為List
sb_youtube_info = data.values.tolist()

for sb_youtube_info_line in sb_youtube_info:

	try:
		country = sb_youtube_info_line[0]
		sb_rank = sb_youtube_info_line[1]
		url = sb_youtube_info_line[2]
		headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
		time.sleep(1)
		response = requests.get(url, headers=headers)
		soup = BeautifulSoup(response.text, 'lxml')   # 使用lxml解析器, 回傳內容裡的文字, 也就是html內容傳入BeautifulSoup

	except BaseException as error:
		print('Expection Error:', error)

	else:
		channel_url = soup.select('#YouTubeUserTopSocial > div > .-margin')[0].attrs['href']

		top_soup = soup.select('#YouTubeUserTopInfoBlockTop')[0]
		channel_name = top_soup.select('div > h1')[0].text
		channel_type = top_soup.select('#youtube-user-page-channeltype')[0].text
		uploads = top_soup.select('#youtube-stats-header-uploads')[0].text
		subs = top_soup.select('#youtube-stats-header-subs')[0].text
		video_views = top_soup.select('#youtube-stats-header-views')[0].text

		rank_info = soup.find(style = re.compile('float: right; width'))
		type_rank = rank_info.select('div:nth-child(5) > div:nth-child(1) > p')[0].text

		youtube_link = [country, sb_rank, channel_name, channel_type, type_rank, uploads, subs, video_views, channel_url]
		youtube_info.append(youtube_link)
		
# to DataFrame
youtube_info_df = pd.DataFrame(youtube_info, columns=['country', 'sb_rank', 'channel_name', 'channel_type', 'type_rank', 'uploads', 'subs', 'video_views', 'channel_url'])
		
# to excel file
youtube_info_df.to_csv('youtube_info.csv', index=False, sep=',')

# 連結/建立DB
with sqlite3.connect('youtubers.db') as conn:
	youtube_info_df.to_sql('youtube_info', conn, index=False, if_exists='append') 



# country_dict = {'us':'United States', 
# 				'de':'Germany',
# 				'fr':'France',
# 				'jp':'Japan',
# 				'es':'Spain',
# 				'gb':'Great Britain',
# 				'au':'Australia',
# 				'tw':'Taiwan',
# 				'kr':'Korea South',
# 				'it':'Italy'}



