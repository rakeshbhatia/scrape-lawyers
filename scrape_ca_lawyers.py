import re
import os
import time
import json
import string
import random
import asyncio
import requests
import zipcodes
import itertools
import tldextract
import numpy as np
import pandas as pd
import multiprocessing
from math import ceil
from urllib.parse import urlparse
import undetected_chromedriver.v2 as uc
from multiprocessing import Pool
from multiprocessing import Process
from multiprocessing.pool import ThreadPool
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from pprint import pprint
from bs4 import BeautifulSoup
from PyPDF2 import PdfFileReader
from fake_useragent import UserAgent
from requests_html import HTMLSession
from requests_html import AsyncHTMLSession
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.select import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC

class AttorneyScraper:
	def __init__(self, dem_url, da_url):
		self.dem_url = dem_url
		self.da_url = da_url
		self.lawyers = pd.DataFrame(columns=["Name", "Status", "Number", "City", "Admission Date"])
		self.lawyers_data = pd.DataFrame(columns=["Name", "Status", "Number", "City", "Admission Date", "Calbar Website", "Address", "Phone", "Fax", "Email", "Website"])
		self.zip_codes = pd.read_csv("zip-codes-ca.csv")
		self.zip_codes_basic = pd.read_csv("zip-codes-ca-basic.csv")
		self.zip_codes_advanced = pd.read_csv("zip-codes-ca-advanced.csv")
		self.bing_ua = "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"
		self.bing_referrer = "www.bing.com"
		#self.alphabet_pairs = list(itertools.product(list(string.ascii_lowercase), repeat=2))

	def get_user_agent(self):
		# Instantiate random user agent
		ua = UserAgent()
		user_agent = ua.random
		return user_agent

	def get_soup(self, url):
		# Set headers
		#ua = UserAgent()
		#user_agent = ua.random
		headers = {"User-Agent":self.get_user_agent()}

		# Request page and create soup object
		#page = requests.get(url, headers=headers)
		page = requests.get(url)
		soup = BeautifulSoup(page.content, "html.parser")
		return soup

	def browser(self):
		# Instantiate chromedriver options
		options = uc.ChromeOptions()

		# Add options arguments
		options.add_argument('--headless')
		options.add_argument(f"user-agent={self.get_user_agent()}")

		# Instantiate driver
		driver = uc.Chrome(options=options, use_subprocess=True)
		return driver

	def get_zip_codes(self):
		# Get new driver
		driver = self.browser()

		# Get CalBar demographics search URL
		driver.get(self.dem_url)
		time.sleep(3)

		# Select zip code option
		select_zip_code = driver.find_element(By.ID, "rbZipCode")
		select_zip_code.click()
		time.sleep(3)

		# Click search button
		submit = driver.find_element(By.ID, "btnSubmit")
		submit.click()
		time.sleep(10)

		# Get table of zip codes
		tables = pd.read_html(driver.page_source)
		df = tables[1]

		print(df.head())
		print(df.tail())
		print(len(df))

		# Write zip code data to CSV file
		df.to_csv("zip-codes-ca.csv")

		# Quit driver
		driver.quit()

	def clean_zip_codes(self):
		# Read zip codes CSV file
		df = pd.read_csv("zip-codes-ca.csv")

		# Filter out rows containing "Total" data
		df = df[df["Zip Code"] != "Total"]

		print(df.head())
		print(df.tail())
		print(len(df))

		# Write cleaned zip code data to CSV file
		df.to_csv("zip-codes-ca.csv")

	def search_lawyer_basic(self, zip_code):
		print("Scraping lawyers for zip code: ", zip_code)

		# Sleep between 0 and 3 seconds
		#time.sleep(random.randint(0, 3))

		# Generate custom search URL for the zip code
		url = "https://apps.calbar.ca.gov/attorney/LicenseeSearch/AdvancedSearch?LastNameOption=b&LastName=" \
			  "&FirstNameOption=b&FirstName=&MiddleNameOption=b&MiddleName=&FirmNameOption=b&FirmName=&City" \
			  "Option=b&City=&State=&Zip={}&District=&County=&LegalSpecialty=&LanguageSpoken=&PracticeArea=" \
			  .format(zip_code)

		soup = self.get_soup(url)
		#print(soup.prettify())
		tables = pd.read_html(soup.prettify())
		#print(tables)

		# Check if any results were found
		if len(tables) < 4:
			print("No results")
		else:
			data = tables[3]
			#print(data.head())
			self.lawyers = self.lawyers.append(data, ignore_index=True)
			#print(self.lawyers.head())
			self.lawyers.to_csv("lawyers-ca-basic-1.csv", mode="a", index=False, header=False)

	def search_lawyer_advanced(self, zip_code):
		print("Scraping lawyers for zip code: ", zip_code)
		#print("The current zip code has more than 500 entries")

		alphabet_string = string.ascii_lowercase
		alphabet_list = list(alphabet_string)

		for x in itertools.product(alphabet_list, repeat=2):
			print("x[0], x[1]: {}, {}".format(x[0], x[1]))
			# Generate custom search URL for the zip code
			url = "https://apps.calbar.ca.gov/attorney/LicenseeSearch/AdvancedSearch?LastNameOption=b&LastName={}" \
				  "&FirstNameOption=b&FirstName={}&MiddleNameOption=b&MiddleName=&FirmNameOption=b&FirmName=&City" \
				  "Option=b&City=&State=&Zip={}&District=&County=&LegalSpecialty=&LanguageSpoken=&PracticeArea=" \
				  .format(x[0], x[1], zip_code)

			# Get soup object
			soup = self.get_soup(url)
			tables = pd.read_html(soup.prettify())
			#print(tables)

			# Check if any results were found
			if len(tables) < 4:
				print("No results")
				continue
			else:
				data = tables[3]
				self.lawyers = self.lawyers.append(data, ignore_index=True)
				self.lawyers.to_csv("lawyers-ca-advanced-1.csv", mode="a", index=False, header=False)

	def search_lawyer_advanced_alt(self, zip_code, first_initial, last_initial):
		# Sleep between 0 and 3 seconds
		#time.sleep(random.randint(0, 3))

		print("The current zip code has more than 500 entries")
		print("first_initial, last_initial: {}, {}".format(first_initial, last_initial))

		# Generate custom search URL for the zip code
		url = "https://apps.calbar.ca.gov/attorney/LicenseeSearch/AdvancedSearch?LastNameOption=b&LastName={}" \
			  "&FirstNameOption=b&FirstName={}&MiddleNameOption=b&MiddleName=&FirmNameOption=b&FirmName=&City" \
			  "Option=b&City=&State=&Zip={}&District=&County=&LegalSpecialty=&LanguageSpoken=&PracticeArea=" \
			  .format(last_initial, first_initial, zip_code)

		# Get soup object
		soup = self.get_soup(url)
		tables = pd.read_html(soup.prettify())
		#print(tables)

		# Check if any results were found
		if len(tables) < 4:
			print("No results")
		else:
			data = tables[3]
			self.lawyers = self.lawyers.append(data, ignore_index=True)
			self.lawyers.to_csv("lawyers-ca-advanced-1.csv", mode="a", index=False, header=False)

	def search_all_lawyers_basic(self):
		print("Basic search of all active lawyers")
		# Create and configure process pool
		with Pool(processes=8) as pool:
			# Issue tasks to process pool
			pool.imap(self.search_lawyer_basic, self.zip_codes_basic["Zip Code"].to_list())
			# Shutdown process pool
			pool.close()
			# Wait for all issued tasks to complete
			pool.join()

	def search_all_lawyers_advanced(self):
		print("Advanced search of all active lawyers")
		with Pool(processes=8) as pool:
			# Issue tasks to process pool
			pool.imap(self.search_lawyer_advanced, self.zip_codes_advanced["Zip Code"].to_list())
			# Shutdown process pool
			pool.close()
			# Wait for all issued tasks to complete
			pool.join()

	def process_lawyer_data(self):
		alphabet_pairs = list(itertools.product(list(string.ascii_lowercase), repeat=2))

		print(alphabet_pairs)
		print(len(alphabet_pairs))

		'''df = pd.read_csv("zip-codes-ca.csv")

		df1 = df.loc[df["Active"] < 500]
		df2 = df.loc[df["Active"] >= 500]

		print(len(df1))
		print(len(df2))

		df1.to_csv("zip-codes-ca-basic.csv")
		df2.to_csv("zip-codes-ca-advanced.csv")'''

		'''base_url = "https://apps.calbar.ca.gov/attorney/Licensee/Detail/"

		df1 = pd.read_csv("lawyers-ca-basic-1.csv")
		df2 = pd.read_csv("lawyers-ca-advanced-2.csv")

		df1 = df1.loc[df1["Status"] == "Active"]
		df2 = df2.loc[df2["Status"] == "Active"]

		df1 = df1.reset_index(drop=True)
		df2 = df2.reset_index(drop=True)

		df = df1.append(df2, ignore_index=True)

		df = df.reset_index(drop=True)

		df = df.sort_values(by=["City", "Name"], ignore_index=True)

		df = df.reset_index(drop=True)

		df = df.astype(str)

		df = df.drop_duplicates(subset=["Number"], keep="first", ignore_index=True)

		df = df.reset_index(drop=True)

		df["City"] = df["City"].str.lower()
		df["City"] = df["City"].str.title()

		df.to_csv("lawyers-ca.csv")

		df["Calbar Website"] = base_url + df["Number"]

		print(df.head())
		print(df.tail())
		print(len(df))

		df.to_csv("lawyers-ca.csv")'''

	def scrape_lawyer(self, record):
		print("Scraping entry: ", record[0])
		print("Scraping lawyer: ", record[1])
		print("Scraping url: ", record[6])

		# Get soup object
		soup = self.get_soup(record[6])

		# Initialize dictionary
		data = {"Name":record[1], "Status":record[2], "Number":record[3], "City":record[4], "Admission Date":record[5], "Calbar Website":record[6], "Address":"", "Phone":"", "Fax":"", "Email":"", "Website":""}

		# Get visible id for email
		email = ""
		pattern = r"#(e\d){display:inline;}"
		visible_id = re.findall(pattern, soup.prettify())
		count = 0
		while visible_id == []:
			if count < 20:
				#print("visible_id is none")
				soup = self.get_soup(record[6])
				visible_id = re.findall(pattern, soup.prettify())
			else:
				break
			count += 1

		# Get info tag
		info = soup.find_all("div", attrs={"style":"margin-top:1em;"})[1]

		#info = soup.find_all("p")[4]
		#print("info.text: ", info.text)

		# Get address
		data["Address"] = info.find_all("p")[0].text.replace("Address: ", "").strip()

		# Get phone
		data["Phone"] = info.find_all("p")[1].text.split(" ", 1)[1][:14].strip()

		# Get fax
		data["Fax"] = info.find_all("p")[1].text.split(" ", 1)[1][:14].strip()

		# Get email
		if "Email: Not Available" in info.text:
			email = "Not Available"
		else:
			email = info.find("span", attrs={"id":visible_id[0]})

		if email == None or email == "" or email == "Not Available":
			data["Email"] = "Not Available"
		else:
			data["Email"] = email.text

		# Get website
		website = info.find("a", attrs={"id":"websiteLink"})

		if website == None:
			data["Website"] = "Not Available"
		else:
			data["Website"] = website.text

		#print("data: ", data)

		# Append data to instance dataframe
		#self.lawyers_data = self.lawyers_data.append(data, ignore_index=True)
		#print(self.lawyers_data.head())

		# Create DataFrame with single row
		df = pd.DataFrame(columns=["Name", "Status", "Number", "City", "Admission Date", "Calbar Website", "Address", "Phone", "Fax", "Email", "Website"])
		df = df.append(data, ignore_index=True)
		#df = pd.DataFrame(data=data, columns=["Name", "Status", "Number", "City", "Admission Date", "Calbar Website", "Address", "Phone", "Fax", "Email", "Website"])
		#print(df.head())

		# Append data to CSV file
		df.to_csv("lawyers-ca-data.csv", mode="a", index=False, header=False)

	def scrape_all_lawyers(self):
		# Load lawyers list
		df = pd.read_csv("lawyers-ca.csv")
		df = df.astype(str)

		# Convert df to list of tuples
		records = list(df.to_records(index=True))

		# Create and configure process pool
		with Pool(processes=8) as pool:
			# Issue tasks to process pool
			results = pool.imap(self.scrape_lawyer, records)
			# Shutdown process pool
			pool.close()
			# Wait for all issued tasks to complete
			pool.join()

		#self.lawyers_data.to_csv("lawyers-ca-data-1.csv")

	def clean_lawyers_data1(self):
		df = pd.read_csv("ca-lawyers-data.csv")

		# Extract all lawyers with website
		df1a = df.loc[df["Website"] != "Not Available"]
		df1 = df1a.copy()

		#df1["Address"] = df1["Address"].str.lower()
		#df1["Address"] = df1["Address"].str.upper()
		#df1["Email"] = df1["Email"].str.lower()
		df1["Website"] = df1["Website"].str.lower()
		#df1["Website"] = df1["Website"].apply(lambda x: x.lower())

		#df["Website"] = np.where(df["Website"].str.startswith("http"), df["Website"], "https://" + df["Website"])

		#df1["Website"] = df1["Website"].where(df1["Website"].str.startswith("http"), "http://" + df1["Website"])

		#df['A'] = (df['A'] / 2).where(df['A'] < 20, df['A'] * 2)


		# Remove lawyers with unwanted websites
		unwanted = ["checkerspot.com", "rocketmail.com", "protonmail.com", "earthlink.net", "sbcglobal.net", "samsung.com", 
					"facebook.com", "linkedin.com", "verizon.net", "verizon.com", "comcast.net", "hotmail.com", 
					"outlook.com", "pacbell.net", "disney.com", "cydcor.com", "icloud.com", "yandex.com", "yahoo.com", 
					"gmail.com", "sonic.net", "zoho.com", "mail.com", "msn.com", "att.net", "aol.com", "gmx.com", 
					"hpe.com", "me.com", "cs.com", "gmx.us", "bit.ly", "ca.us", ".edu", ".gov", ".mil"]

		df1 = df1[~df1["Website"].str.contains("|".join(unwanted))]

		# Sort by city
		df1 = df1.sort_values(by=["City"], ignore_index=True)

		df1 = df1.reset_index(drop=True)

		#print(df1.head())
		#print(df1.tail())
		#print(len(df1))

		#df1.to_csv("ca-lawyers-with-website-1.csv")

		# Extract all lawyers without website and with email
		df2a = df.loc[df["Website"] == "Not Available"]
		df2 = df2a.copy()

		df2 = df2.loc[df2["Email"] != "Not Available"]
		#df2["Address"] = df2["Address"].str.lower()
		#df2["Address"] = df2["Address"].str.upper()
		df2["Email"] = df2["Email"].str.lower()
		#df2["Email"] = df2["Email"].apply(lambda x: x.lower())
		#df2["Website"] = df2["Website"].str.lower()

		# Remove lawyers with unwanted email providers
		df2 = df2[~df2["Email"].str.contains("|".join(unwanted))]

		'''df2 = df2[~df2["Email"].str.contains("checkerspot.com")]
		df2 = df2[~df2["Email"].str.contains("rocketmail.com")]
		df2 = df2[~df2["Email"].str.contains("protonmail.com")]
		df2 = df2[~df2["Email"].str.contains("earthlink.net")]
		df2 = df2[~df2["Email"].str.contains("sbcglobal.net")]
		df2 = df2[~df2["Email"].str.contains("verizon.net")]
		df2 = df2[~df2["Email"].str.contains("comcast.net")]
		df2 = df2[~df2["Email"].str.contains("hotmail.com")]
		df2 = df2[~df2["Email"].str.contains("outlook.com")]
		df2 = df2[~df2["Email"].str.contains("pacbell.net")]
		df2 = df2[~df2["Email"].str.contains("disney.com")]
		df2 = df2[~df2["Email"].str.contains("cydcor.com")]
		df2 = df2[~df2["Email"].str.contains("icloud.com")]
		df2 = df2[~df2["Email"].str.contains("yandex.com")]
		df2 = df2[~df2["Email"].str.contains("yahoo.com")]
		df2 = df2[~df2["Email"].str.contains("gmail.com")]
		df2 = df2[~df2["Email"].str.contains("sonic.net")]
		df2 = df2[~df2["Email"].str.contains("zoho.com")]
		df2 = df2[~df2["Email"].str.contains("mail.com")]
		df2 = df2[~df2["Email"].str.contains("msn.com")]
		df2 = df2[~df2["Email"].str.contains("att.net")]
		df2 = df2[~df2["Email"].str.contains("aol.com")]
		df2 = df2[~df2["Email"].str.contains("gmx.com")]
		df2 = df2[~df2["Email"].str.contains("hpe.com")]
		df2 = df2[~df2["Email"].str.contains("me.com")]
		df2 = df2[~df2["Email"].str.contains("cs.com")]
		df2 = df2[~df2["Email"].str.contains("gmx.us")]
		df2 = df2[~df2["Email"].str.contains(".edu")]
		df2 = df2[~df2["Email"].str.contains(".gov")]'''

		# Grab domain from email address
		#df2["Website"] = "www." + df2["Email"].apply(lambda x: x.split("@")[1])

		new = df2["Email"].str.split("@", expand = True)

		#print(new.head())

		#print(new[1])

		df2["Website"] = "www." + new[1]

		#df2["Website"] = ("https://" + df2["Website"]).where(df2["Website"].str.startswith("http"), df2["Website"])

		#df2["Website"] = df2["Website"].where(df2["Website"].str.startswith("http"), "http://" + df2["Website"])

		# Sort by city
		df2 = df2.sort_values(by=["City"], ignore_index=True)

		df2 = df2.reset_index(drop=True)

		#print(df2.head())
		#print(df2.tail())
		#print(len(df2))

		df = df1.append(df2, ignore_index=True)

		df["Law Firm"] = ""

		df["DA Score"] = ""

		df = df[["Law Firm", "City", "Website", "DA Score"]].copy()

		print(df.dtypes)

		df = df[df["Website"] != "None"]
		df = df[df["Website"] != "none"]
		df = df[df["Website"] != ""]

		df["Website"] = df["Website"].astype(str)

		#df["Website"] = df["Website"].apply(lambda x: urlparse(x).netloc)

		df["Website"] = df["Website"].apply(lambda x: ".".join(part for part in tldextract.extract(x) if part))

		#df = df.assign(Website = lambda x: urlparse(x).netloc)

		print(df.head())
		print(df.tail())
		print(len(df))

		df = df.drop_duplicates(subset=["Website"], keep="first", ignore_index=True)

		df = df.sort_values(by=["City"], ignore_index=True)

		df = df.reset_index(drop=True)

		#for i in range(len(df)):
		#	split = urlparse(df.loc[i, "Website"])
		#	df.loc[i, "Website"] = split.netloc

		print(df.head())
		print(df.tail())
		print(len(df))

		df.to_csv("ca-law-firms-with-website.csv")

	def clean_lawyers_data2(self):
		df = pd.read_csv("ca-lawyers-data.csv", na_filter=False)

		# Extract all lawyers with website
		df1a = df.loc[df["Website"] != "Not Available"]
		df1 = df1a.copy()

		df1["Website"] = df1["Website"].str.lower()

		# Remove lawyers with unwanted websites
		unwanted = ["checkerspot.com", "rocketmail.com", "protonmail.com", "earthlink.net", "sbcglobal.net", "samsung.com", 
					"facebook.com", "linkedin.com", "verizon.net", "verizon.com", "comcast.net", "hotmail.com", "anaheim.net", 
					"outlook.com", "pacbell.net", "disney.com", "cydcor.com", "icloud.com", "yandex.com", "yahoo.com", 
					"gmail.com", "sonic.net", "zoho.com", "mail.com", "msn.com", "att.net", "aol.com", "gmx.com", 
					"hpe.com", "me.com", "cs.com", "gmx.us", "bit.ly", "ca.us", ".edu", ".gov", ".mil"]

		df1 = df1[~df1["Website"].str.contains("|".join(unwanted))]

		# Sort by city
		df1 = df1.sort_values(by=["City"], ignore_index=True)

		df1 = df1.reset_index(drop=True)

		#print(df1.head())
		#print(df1.tail())
		#print(len(df1))

		df1["Law Firm"] = ""

		df1["DA Score"] = ""

		df2 = df1[["Law Firm", "City", "Website", "DA Score"]].copy()

		print(df2.dtypes)

		df2 = df2[df2["Website"] != "None"]
		df2 = df2[df2["Website"] != "none"]
		df2 = df2[df2["Website"] != ""]

		df2["Website"] = df2["Website"].astype(str)

		df2["Website"] = df2["Website"].apply(lambda x: ".".join(part for part in tldextract.extract(x) if part))

		df2["Website"] = df2["Website"].str.replace("www.", "")

		print(df2.head())
		print(df2.tail())
		print(len(df2))

		df2 = df2.drop_duplicates(subset=["Website"], keep="first", ignore_index=True)

		df2 = df2.sort_values(by=["City"], ignore_index=True)

		df2 = df2.reset_index(drop=True)

		print(df2.head())
		print(df2.tail())
		print(len(df2))

		df2.to_csv("ca-law-firms-with-website-only.csv", index=False)

	def more_cleaning(self):
		df1 = pd.read_csv("ca-law-firms-with-website-1.csv")
		df1 = df1[~df1["Website"].str.contains(".gov")]
		df1 = df1[~df1["Website"].str.contains(".edu")]
		df1.to_csv("ca-law-firms-with-website-2.csv", index=False)

		df2 = pd.read_csv("da-scores-1.csv")
		df2 = df2[~df2["Website"].str.contains(".gov")]
		df2 = df2[~df2["Website"].str.contains(".edu")]
		df2.to_csv("da-scores-2.csv", index=False)

	def scrape_da_score1(self, url_string):
		#print("Scraping entry: ", record[0])
		#print("Scraping url: ", record[3])

		#data = {"Law Firm":"", "City":record[2], "Website":record[3], "DA Score":""}

		#url = "www.paytonemploymentlaw.com"

		time.sleep(random.randint(0, 3))

		# Get new driver
		driver = self.browser()
		driver.get(self.da_url)

		time.sleep(1)

		driver.find_element(By.ID, "urls").send_keys(url_string)

		#search = driver.find_element(By.ID, "urls")
		#search.send_keys(url_string)

		time.sleep(1)

		#search.send_keys(Keys.RETURN)

		driver.find_element(By.ID, "exclude_url").click()

		time.sleep(1)

		driver.find_element(By.ID, "checkBtnCap").click()

		#submit = driver.find_element(By.ID, "checkBtnCap")
		#submit.click()

		#time.sleep(10)

		#element = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.ID, "example")))

		'''try:
			element = WebDriverWait(driver, 30).until(
				EC.presence_of_element_located((By.CLASS_NAME, "display table"))
			)
			print(element.text)
			df = pd.read_html(driver.page_source)[0]
			print(df.head())
			print(len(df))
			df.to_csv("da-scores-final-test.csv", mode="a", index=False, header=False)
		finally:
			driver.quit()'''

		time.sleep(10)

		df = pd.read_html(driver.page_source)[0]
		print(df.head())
		print(len(df))

		if len(df) < 10:
			time.sleep(20)
			df = pd.read_html(driver.page_source)[0]
			print(df.head())
			print(len(df))

		driver.quit()

		# Append data to CSV file
		df.to_csv("da-scores-final-test.csv", mode="a", index=False, header=False)

	def scrape_all_da_scores1a(self):
		print("Scraping all DA scores")

		# Load law firm links
		df = pd.read_csv("ca-law-firms-with-website.csv")
		df = df.astype(str)

		#urls = df["Website"].tolist()[:200]

		urls = df["Website"].tolist()[:40]
		url_strings = []

		for i in range(0, len(urls), 20):
			url_string = "\n".join(urls[i:i+20])
			url_strings.append(url_string)

		print(url_strings)

		for url_string in url_strings:
			#time.sleep(random.randint(0, 3))
			self.scrape_da_score(url_string)

	def scrape_all_da_scores1b(self):
		print("Scraping all DA scores")

		# Load law firm links
		df = pd.read_csv("ca-law-firms-with-website.csv")
		df = df.astype(str)

		urls = df["Website"].tolist()[:200]
		url_strings = []

		for i in range(0, len(urls), 20):
			url_string = "\n".join(urls[i:i+20])
			url_strings.append(url_string)

		# Create and configure process pool
		with Pool(processes=2) as pool:
			#n_tasks = len(urls)
			#n_tasks_per_chunk = ceil(n_tasks/2)
			# Issue tasks to process pool
			#results = pool.imap(self.scrape_da_score3, urls, chunksize=n_tasks_per_chunk)
			results = pool.imap(self.scrape_da_score, url_strings)
			# Shutdown process pool
			pool.close()
			# Wait for all issued tasks to complete
			pool.join()

	def scrape_da_score2(self, url_string):
		time.sleep(random.randint(1, 3))

		print(url_string)

		# Get new driver
		driver = self.browser()
		driver.get(self.da_url)

		time.sleep(1)

		search = driver.find_element(By.ID, "search_text")
		search.send_keys(url_string)

		time.sleep(1)

		search.send_keys(Keys.RETURN)

		#submit = driver.find_element(By.CLASS_NAME, "btn btn-danger")
		#submit.click()

		#time.sleep(3)

		xpath = "/html/body/section[2]/div/div[1]/div/table/tbody"
		css_selector = "#preview > tbody:nth-child(1)"

		try:
			element = WebDriverWait(driver, 100).until(
				EC.presence_of_element_located((By.XPATH, xpath))
			)
			print(element.text)
			df = pd.read_html(driver.page_source)[0]

			print(df.head())
			print(df.tail())
			print(len(df))

			#df.columns = df.iloc[0]

			header_row = df.iloc[0]
			df = pd.DataFrame(df.values[1:], columns=header_row)

			df.insert(0, "Website", url_string.split(","))

			print(df.head())
			print(df.tail())
			print(len(df))

			df.to_csv("accessily-da-scores.csv", mode="a", index=False, header=False)
		finally:
			driver.quit()

		'''df = pd.read_html(driver.page_source)[0]

		print(df.head())
		print(df.tail())
		print(len(df))

		#df.columns = df.iloc[0]

		header_row = df.iloc[0]
		df = pd.DataFrame(df.values[1:], columns=header_row)

		df.insert(0, "Website", url_string.split(","))

		print(df.head())
		print(df.tail())
		print(len(df))

		driver.quit()

		# Append data to CSV file
		df.to_csv("accessily-da-scores.csv", mode="a", index=False, header=False)'''

	def scrape_all_da_scores2a(self):
		print("Scraping all DA scores")

		# Load law firm links
		df = pd.read_csv("missing-da-scores.csv")
		df = df.astype(str)

		urls = df["Website"].tolist()
		url_strings = []

		for i in range(0, len(urls), 10):
			url_string = ",".join(urls[i:i+10])
			url_strings.append(url_string)

		print(url_strings)

		for url_string in url_strings:
			self.scrape_da_score2(url_string)

	def scrape_all_da_scores2b(self):
		print("Scraping all DA scores")

		# Load law firm links
		df = pd.read_csv("missing-da-scores.csv")
		df = df.astype(str)

		urls = df["Website"].tolist()[:20]
		url_strings = []

		for i in range(0, len(urls), 10):
			url_string = ",".join(urls[i:i+10])
			url_strings.append(url_string)

		workers = 2

		# Create and configure process pool
		with Pool(processes=workers) as pool:
			n_tasks = len(url_strings)
			n_tasks_per_chunk = ceil(n_tasks/workers)
			# Issue tasks to process pool
			results = pool.imap(self.scrape_da_score3, url_strings, chunksize=n_tasks_per_chunk)
			#results = pool.imap(self.scrape_da_score, url_strings)
			# Shutdown process pool
			pool.close()
			# Wait for all issued tasks to complete
			pool.join()

	def process_data1(self):
		df = pd.read_csv("ca-law-firms-with-website-3.csv")
		#print(df.dtypes)

		df = df[df["DA Score"] >= 50]
		df = df.reset_index(drop=True)

		print(df.head())
		print(df.tail())
		print(len(df))

		df.to_csv("ca-law-firms-with-website-4.csv", index=False)

	def process_data2(self):
		df = pd.read_csv("ca-law-firms-with-website-3.csv")
		#print(df.dtypes)

		df = df[df["DA Score"] >= 30]
		df = df.reset_index(drop=True)

		print(df.head())
		print(df.tail())
		print(len(df))

		df.to_csv("ca-law-firms-with-website-da-score-30.csv", index=False)

	def scrape_law_firm_name(self, record):
		print("Scraping entry: ", record[0])
		print("Scraping url: ", record[3])

		# Initialize dictionary
		data = {"Law Firm":"", "City":record[2], "Website":record[3], "DA Score":record[4]}

		#print(record[3])

		# Get soup object
		soup = self.get_soup("https://" + record[3])

		#print(soup.prettify())

		title = soup.find("meta", property="og:title")
		#title_tag = soup.find("title")

		print("title: ", title)

		if title != None:
			print(title["content"])
			data["Law Firm"] = title["content"]
		else:
			title = soup.find("title")
			if title != None:
				print(title.text)
				data["Law Firm"] = title.text
			else:
				data["Law Firm"] = ""

		#image = soup.find("meta", name="og:image")

		#print(title["content"] if title else "No meta title given")
		#print(image["content"]if title else "No meta title given")

		# Create DataFrame with single row
		df = pd.DataFrame(columns=["Law Firm", "City", "Website", "DA Score"])
		df = df.append(data, ignore_index=True)

		# Append data to CSV file
		df.to_csv("ca-law-firms-with-website-6.csv", mode="a", index=False, header=False)

	def scrape_all_law_firm_names(self):
		df = pd.read_csv("ca-law-firms-with-website-5.csv")
		#df = df.astype(str)

		# Convert df to list of tuples
		records = list(df.to_records(index=True))

		# Create and configure process pool
		with Pool(processes=8) as pool:
			# Issue tasks to process pool
			results = pool.imap(self.scrape_law_firm_name, records)
			# Shutdown process pool
			pool.close()
			# Wait for all issued tasks to complete
			pool.join()

	def final_processing1(self):
		df1 = pd.read_csv("ca-law-firms-with-website.csv")
		df2 = pd.read_csv("da-scores-softo.csv")

		'''print(df1.head())
		print(df1.tail())
		print(len(df1))

		print(df2.head())
		print(df2.tail())
		print(len(df2))'''	

		#df1.loc[df1["Website"].isin(df2["Website"]), "DA Score"] = df2.loc[df2["Website"].isin(df1["Website"]), "DA Score"].values
		#df1.loc[df1["Website"].isin(df2["Website"]), "DA Score"] = df2["DA Score"].values

		df = df1.merge(df2, how="outer", on=["Website"])
		#df = df1.merge(df2, how="outer", left_on="Website", right_on="Website")

		df = df1.merge(df2, how="left")

		print(df.head())
		print(df.tail())
		print(len(df))

		df.to_csv("ca-law-firms-with-website-final.csv")

		#for i in range(10):
		#	print(df2.loc[df2["Website"] == df1.loc[i, "Website"], "DA Score"].item())
		#	df1.loc[i, "DA Score"] = df2.loc[df2["Website"] == df1.loc[i, "Website"], "DA Score"]

		#df = df.sort_values(by=["City"], ignore_index=True)

	def get_missing_da_scores(self):
		df1 = pd.read_csv("ca-law-firms-with-website-final.csv")
		df1 = df1.astype(str)
		df2 = pd.DataFrame(columns=["Law Firm", "City", "Website", "Alternate Website", "DA Score"])

		#print(df1.head())
		#print(len(df1))

		for i in range(len(df1)):
			if df1.loc[i, "DA Score"] == "nan":
				#print(df1.loc[i, "Website"])
				df2 = df2.append(df1.iloc[i], ignore_index=True)

		df2.to_csv("missing-da-scores.csv", index=False)

	def final_processing2(self):
		df1 = pd.read_csv("ca-law-firms-with-website-final.csv", na_filter=False)
		df2 = pd.read_csv("accessily-da-scores.csv", na_filter=False)

		df1 = df1.astype(str)
		df2 = df2.astype(str)

		#print(df1.dtypes)
		#print(df2.dtypes)

		#df1.loc[df1["Website"].isin(df2["Website"])] = df2["Website"].tolist()

		#df = df1.loc[df1["Website"] == df2["Website"]]
		#df = df1.loc[df1["Website"].isin(df2["Website"])]

		#print(df.head())
		#print(df.tail())
		#print(len(df))

		urls = df2["Website"].tolist()
		da_scores = df2["DA Score"].tolist()

		#print(len(urls))
		#print(len(da_scores))

		for i in range(len(urls)):
			print("Before")
			print(df1.loc[df1["Website"] == urls[i], "DA Score"])
			df1.loc[df1["Website"] == urls[i], "DA Score"] = da_scores[i]
			print("After")
			print(df1.loc[df1["Website"] == urls[i], "DA Score"])

		print(df1.head())
		print(df1.tail())
		print(len(df1))

		df1.to_csv("ca-law-firms-with-website-master.csv", index=False)

	def final_processing3(self):
		df = pd.read_csv("ca-law-firms-with-website-master.csv", na_filter=False)
		print(df.dtypes)

		df["Website"] = df["Website"].str.replace("www.", "")
		df["Alternate Website"] = df["Alternate Website"].str.replace("www.", "")

		df = df.drop_duplicates(subset=["Website"], keep="first", ignore_index=True)

		print(df.head())
		print(df.tail())
		print(len(df))

		df.to_csv("ca-law-firms-with-website-master.csv", index=False)

	def final_processing4(self):
		df = pd.read_csv("ca-law-firms-with-website-master.csv", na_filter=False)
		
		df["DA Score"] = df["DA Score"].astype(int)
		print(df.dtypes)

		df = df[df["DA Score"] >= 30]
		df = df.reset_index(drop=True)

		print(df.head())
		print(df.tail())
		print(len(df))

		df.to_csv("ca-law-firms-with-website-final-da-score-30.csv", index=False)

	def final_processing5(self):
		df1 = pd.read_csv("ca-law-firms-with-website-only.csv", na_filter=False)
		df2 = pd.read_csv("ca-law-firms-with-website-master.csv", na_filter=False)

		df1 = df1.astype(str)
		df2 = df2.astype(str)

		urls = df2["Website"].tolist()
		da_scores = df2["DA Score"].tolist()

		#print(len(urls))
		#print(len(da_scores))

		for i in range(len(urls)):
			#print("Before")
			#print(df1.loc[df1["Website"] == urls[i], "DA Score"])
			df1.loc[df1["Website"] == urls[i], "DA Score"] = da_scores[i]
			#print("After")
			#print(df1.loc[df1["Website"] == urls[i], "DA Score"])

		print(df1.head())
		print(df1.tail())
		print(len(df1))

		df1.to_csv("ca-law-firms-with-website-only-final.csv", index=False)

	def final_processing6(self):
		df = pd.read_csv("ca-law-firms-with-website-only-final.csv", na_filter=False)
		
		df["DA Score"] = df["DA Score"].astype(int)
		print(df.dtypes)

		df = df[df["DA Score"] >= 30]
		df = df.reset_index(drop=True)

		print(df.head())
		print(df.tail())
		print(len(df))

		df.to_csv("ca-law-firms-with-website-only-final-da-score-30.csv", index=False)

	def final_processing7(self):
		df = pd.read_csv("ca-lawyers-data.csv", na_filter=False)

		print(df.head())
		print(df.tail())
		print(len(df))

		df = df[df["Website"] != "Not Available"]
		df = df.reset_index(drop=True)

		print(df.head())
		print(df.tail())
		print(len(df))

		df.to_csv("ca-lawyers-with-website-only-final.csv", index=False)

	def final_processing8(self):
		df = pd.read_csv("ca-lawyers-with-website-only-final.csv", na_filter=False)

		df["Email"] = df["Email"].str.lower()

		df["Website"] = df["Website"].apply(lambda x: ".".join(part for part in tldextract.extract(x) if part))
		df["Website"] = df["Website"].str.replace("www.", "")
		df["Website"] = df["Website"].str.lower()

		# Remove lawyers with unwanted websites
		unwanted = ["checkerspot.com", "rocketmail.com", "protonmail.com", "earthlink.net", "sbcglobal.net", "samsung.com", 
					"facebook.com", "linkedin.com", "verizon.net", "verizon.com", "comcast.net", "hotmail.com", 
					"outlook.com", "pacbell.net", "disney.com", "cydcor.com", "icloud.com", "yandex.com", "yahoo.com", 
					"gmail.com", "sonic.net", "zoho.com", "mail.com", "msn.com", "att.net", "aol.com", "gmx.com", 
					"hpe.com", "me.com", "cs.com", "gmx.us", "bit.ly", "ca.us", ".edu", ".gov", ".mil"]

		df = df[~df["Website"].str.contains("|".join(unwanted))]

		# Sort by city
		df = df.sort_values(by=["City"], ignore_index=True)
		df = df.reset_index(drop=True)

		#address = df["Address"].str.split(",", expand=True)
		pattern = r"^(\D*)\d"
		df["Law Firm"] = df["Address"].str.extract(pattern, expand=False)
		df["Law Firm"] = df["Law Firm"].str[:-2]
		df["Law Firm"] = df["Law Firm"].str.replace(", PO Bo", "")

		df = df[["Name", "Law Firm", "City", "Email", "Website"]].copy()

		print(df.head())
		print(df.tail())
		print(len(df))

		'''df1 = df[df["Address Part 1"] == ""]

		print(df1.head())
		print(df1.tail())
		print(len(df1))

		df2 = df[df["Email"] == "Not Available"]

		print(df2.head())
		print(df2.tail())
		print(len(df2))'''

		df.to_csv("ca-lawyers-with-website-only-final-cleaned.csv", index=False)

	def final_processing9(self):
		df1 = pd.read_csv("ca-lawyers-with-website-only-final-cleaned.csv", na_filter=False)
		df2 = pd.read_csv("ca-law-firms-with-website-only-final-da-score-30.csv", na_filter=False)
		
		df1["DA Score"] = ""

		df1 = df1.astype(str)
		df2 = df2.astype(str)

		urls = df2["Website"].tolist()
		da_scores = df2["DA Score"].tolist()

		#print(len(urls))
		#print(len(da_scores))

		for i in range(len(urls)):
			df1.loc[df1["Website"] == urls[i], "DA Score"] = da_scores[i]

		df1 = df1[df1["DA Score"] != ""]

		df1 = df1.sort_values(by=["City", "Website"], ignore_index=True)
		df1 = df1.reset_index(drop=True)

		print(df1.head())
		print(df1.tail())
		print(len(df1))

		df1.to_csv("ca-lawyers-with-website-only-final-cleaned-da-score-30.csv", index=False)

def main():
	start = time.time()

	dem_url = "https://apps.calbar.ca.gov/members/demographics_search.aspx"
	#da_url = "https://www.codingace.com/seo-tools/domain-authority-checker"
	da_url = "https://accessily.com/domain-authority-checker.php"
	#da_url = "https://www.softo.org/tool/domain-authority-checker"
	#da_url = "https://www.dachecker.org"

	#record = (0, "", "Acton", "www.paytonemploymentlaw.com", "")

	scraper = AttorneyScraper(dem_url, da_url)
	#scraper.get_zip_codes()
	#scraper.clean_zip_codes()
	#scraper.search_lawyer_basic("94536")
	#scraper.search_all_lawyers_basic()
	#scraper.search_all_lawyers_advanced()
	#scraper.scrape_all_lawyers()
	#scraper.clean_lawyers_data()
	#scraper.more_cleaning()
	#scraper.scrape_da_score2("www.paytonemploymentlaw.com,albmac.com,www.vialawfirm.com")
	#scraper.scrape_all_da_scores2a()
	#scraper.process_data()
	#scraper.scrape_all_law_firm_names()
	#scraper.process_data2()
	#scraper.final_processing()
	#scraper.get_missing_da_scores()
	#scraper.final_processing4()
	#scraper.clean_lawyers_data2()
	scraper.final_processing9()

	end = time.time()

	execution_time = end - start

	print("Execution time: {} seconds".format(execution_time))

if __name__ == "__main__":
	main()



