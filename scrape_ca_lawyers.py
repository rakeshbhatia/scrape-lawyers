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
		options.add_argument("--headless")
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

		"""df = pd.read_csv("zip-codes-ca.csv")

		df1 = df.loc[df["Active"] < 500]
		df2 = df.loc[df["Active"] >= 500]

		print(len(df1))
		print(len(df2))

		df1.to_csv("zip-codes-ca-basic.csv")
		df2.to_csv("zip-codes-ca-advanced.csv")"""

		"""base_url = "https://apps.calbar.ca.gov/attorney/Licensee/Detail/"

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

		df.to_csv("lawyers-ca.csv")"""

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

		#df["A"] = (df["A"] / 2).where(df["A"] < 20, df["A"] * 2)


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

		"""df2 = df2[~df2["Email"].str.contains("checkerspot.com")]
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
		df2 = df2[~df2["Email"].str.contains(".gov")]"""

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

		"""try:
			element = WebDriverWait(driver, 30).until(
				EC.presence_of_element_located((By.CLASS_NAME, "display table"))
			)
			print(element.text)
			df = pd.read_html(driver.page_source)[0]
			print(df.head())
			print(len(df))
			df.to_csv("da-scores-final-test.csv", mode="a", index=False, header=False)
		finally:
			driver.quit()"""

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

		"""df = pd.read_html(driver.page_source)[0]

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
		df.to_csv("accessily-da-scores.csv", mode="a", index=False, header=False)"""

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

		"""print(df1.head())
		print(df1.tail())
		print(len(df1))

		print(df2.head())
		print(df2.tail())
		print(len(df2))"""	

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
		"""unwanted = ["checkerspot.com", "rocketmail.com", "protonmail.com", "earthlink.net", "sbcglobal.net", "samsung.com", 
					"facebook.com", "linkedin.com", "verizon.net", "verizon.com", "comcast.net", "hotmail.com", 
					"outlook.com", "pacbell.net", "disney.com", "cydcor.com", "icloud.com", "yandex.com", "yahoo.com", 
					"gmail.com", "sonic.net", "zoho.com", "mail.com", "msn.com", "att.net", "aol.com", "gmx.com", 
					"hpe.com", "me.com", "cs.com", "gmx.us", "bit.ly", "ca.us", ".edu", ".gov", ".mil"]"""

		unwanted = ["thecheesecakefactory.com", "roadrunner.com", "checkerspot.com", "rocketmail.com", "protonmail.com", "earthlink.net", "sbcglobal.net", "brightview.com", 
					"samsung.com", "facebook.com", "linkedin.com", "verizon.net", "verizon.com", "comcast.net", "hotmail.com", "outlook.com", 
					"pacbell.net", "disney.com", "cydcor.com", "icloud.com", "yandex.com", "yahoo.com", "gmail.com", "sonic.net", "zoho.com", "qnet.com",
					"mail.com", "msn.com", "att.net", "aol.com", "gmx.com", "hpe.com", "me.com", "cs.com", "gmx.us", "bit.ly", "ca.us", ".edu", 
					".gov", ".mil"]

		df = df[~df["Website"].str.contains("|".join(unwanted))]

		# Sort by city
		df = df.sort_values(by=["City"], ignore_index=True)
		df = df.reset_index(drop=True)

		#address = df["Address"].str.split(",", expand=True)
		pattern = r"^(\D*)\d"
		df["Law Firm"] = df["Address"].str.extract(pattern, expand=False)
		df["Law Firm"] = df["Law Firm"].str[:-2]
		df["Law Firm"] = df["Law Firm"].str.replace(", PO Bo", "")

		df = df[["Name", "City", "Law Firm", "Address", "Email", "Website"]].copy()

		print(df.head())
		print(df.tail())
		print(len(df))

		"""df1 = df[df["Address Part 1"] == ""]

		print(df1.head())
		print(df1.tail())
		print(len(df1))

		df2 = df[df["Email"] == "Not Available"]

		print(df2.head())
		print(df2.tail())
		print(len(df2))"""

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

	def final_processing10(self):
		df = pd.read_csv("ca-lawyers-with-website-only-final-cleaned-da-score-30.csv", na_filter=False)

		df = df[df["Email"] == "not available"]

		print(df.head())
		print(df.tail())
		print(len(df))

	def final_processing11(self):
		df = pd.read_csv("ca-lawyers-data.csv", na_filter=False)
		df = df.astype(str)

		df = df[(df["Email"] != "Not Available") & (df["Website"] == "Not Available")]

		#print(df.dtypes)
		#print(df.head())
		#print(df.tail())
		#print(len(df))

		df["Email"] = df["Email"].str.lower()

		excluded_court = ["occourts.org", "sb-court.org", "lacourt.org", "lasuperiorcourt.org", "yubacourts.org", "mercedcourt.org", "eldoradocourt.org", "sanmateocourt.org", 
						  "sb-courts.org", "scscourt.org", "sbcourts.org", "santacruzcourt.org", "sonomacourt.org", "sjcourts.org", "suttercourts.com"]

		excluded_city = ["alamedacityattorney.org", "bakersfieldcity.us", "cityofberkeley.info", "city-attorney.com", "comptoncity.org", "cityofconcord.org",
						 "crescentcity.org", "culvercity.org", "dalycity.org", "elkgrovecity.org", "cityofgoleta.org", "cityofhawthorne.org", "surfcity-hb.org",
						 "cityoflancasterca.org", "cityoflivermore.net", "lacity.org", "atty.lacity.org", "cityview.com", "cityofmerced.org", "cityofnapa.org",
						 "oaklandcityattorney.org", "cityoforange.org", "cityofpaloalto.org", "cityofpasadena.net", "cityworksinc.org", "cityofpetaluma.org", 
						 "cityofredding.org", "redwoodcity.org", "cityofsacramento.org", "rivercitybank.com", "sfcityatty.org", "slocity.org", "cityofsanmateo.org",
						 "cityofsantamaria.org", "surfcity.net", "srcity.org", "cityofslt.us", "cityofvacaville.com", "cityofvallejo.net", "hacityventura.org", 
						 "cityofwestsacramento.org"]

		excluded_county = ["stancounty.com", "kerncounty.com", "theurbannestnorthcounty.com", "countyofcolusa.com", "countyofcolusa.org", "solanocounty.com", 
						   "solanocountylawyer.com", "mendocinocounty.org", "inyocounty.us", "lacountyda.org", "lacountypubdef.org", "madera-county.com", 
						   "maderacounty.com", "mariposacounty.org", "cc.cccounty.us", "dcss.cccounty.us", "pd.cccounty.us", "countyofmerced.com", "countyofnapa.org", 
						   "nevadacountycourts.com", "marincounty.org", "buttecounty.net", "countyofplumas.com", "saccounty.net", "pd.sbcounty.go", "countyofsb.org", 
						   "santacruzcounty.us", "sonoma-county.org", "mendocinocounty.com", "tularecountylaw.com", "sdcountytrusts.com", "trinitycounty.org", 
						   "countyofglenn.net", "ccm.yolocounty.org", "yolocounty.org", "ventura.org"]

		# Remove lawyers with unwanted emails
		unwanted = [".edu", ".gov", ".mil", "ca.us", "1cbank", "qq.com", "me.com", "cs.com", "gmx.us", "bit.ly", "uhc.com", "wmg.com", "dga.org", "cnb.com", "msn.com", "att.net", "aol.com", "mac.com", "mgm.com", "cox.net", "zorro.com", 
					"gmx.com", "hpe.com", "live.com", "divx.com", "iehp.org", "ah4r.com", "khov.com", "sfhp.org", "cbre.com", "zoho.com", "qnet.com", "scif.com", "mail.com", "mavtv.com", "frbsf.org", "epson.com", "bayer.com", 
					"tesla.com", "yahoo.com", "gmail.com", "intel.com", "sonic.net", "apple.com", "acphd.org", "waltdisney", "edf-re.com", "icumed.com", "davita.com", "abbott.com", "disney.com", "cydcor.com", "mindspring.com", "borregohealth.org", 
					"yandex.com", "google.com", "icloud.com", "tubitv.com", "nvidia.com", "vna.health", "legionm.com", "samsung.com", "calfund.org", "tinyurl.com", "phcdocs.org", "verizon.net", "anaheim.net", "oscars.org", "pmhd.org", 
					"verizon.com", "comcast.net", "hotmail.com", "outlook.com", "udacity.com", "chevron.com", "firstam.com", "agilent.com", "8minute.com", "altamed.org", "hotbench.tv", "pacbell.net", "netscape.net", "studio71us.com", 
					"rivcoda.org", "gvhomes.com", "deloitte.com", "facebook.com", "linkedin.com", "teamwass.com", "qualcomm.com", "bankofamerica", "viacomcbs.com", "paramount.com", "dollskill.com", "theagencyre.com", "unitedtalent.com", 
					"earthlink.net", "sbcglobal.net", "coveredca.com", "viacomcbs.com", "fremantle.com", "ovationtv.com", "peacocktv.com", "lucasfilm.com", "ingrooves.com", "livenation.com", "wmeagency.com", "psln.com", "cbbank.com", 
					"warnerbros.com", "wellsfargo.com", "roadrunner.com", "brightview.com", "rocketmail.com", "protonmail.com", "salesforce.com", "bridgebank.com", "montecito.bank", "silvergate.com", 
					"aidshealth.org", "uclahealth.org", "smartvoter.org", "servicenow.com", "progressive.com", "playstation.com", "virginorbit.com", "aegpresents.com", "checkerspot.com", "kernmedical.com", 
					"hingehealth.com", "denovahomes.com", "homebaseccc.org", "sonypictures.com", "eastwestbank.com", "carbonhealth.com", "nbcuniversal.com", "landseahomes.com", "scrippshealth.org", 
					"commercecasino.com", "universalmusic.com", "relativityspace.com", "adventisthealth.org", "molinahealthcare.com", "mrcentertainment.com", "createmusicgroup.com", "firstfoundationinc.com", 
					"stanfordhealthcare.org", "thecheesecakefactory.com", "entertainmentstudios.com", "discoverybehavioralhealth.com"]

		df = df[~df["Email"].str.contains("|".join(excluded_court))]
		df = df[~df["Email"].str.contains("|".join(excluded_city))]
		df = df[~df["Email"].str.contains("|".join(excluded_county))]
		df = df[~df["Email"].str.contains("|".join(unwanted))]

		new = df["Email"].str.split("@", expand = True)

		df["Website"] = new[1]

		#df = df[~df["Website"].str.contains("|".join(excluded_city))]
		#df = df[~df["Website"].str.contains("|".join(excluded_county))]
		#df = df[~df["Website"].str.contains("|".join(unwanted))]

		#address = df["Address"].str.split(",", expand=True)
		pattern = r"^(\D*)\d"
		df["Law Firm"] = df["Address"].str.extract(pattern, expand=False)
		df["Law Firm"] = df["Law Firm"].str[:-2]
		df["Law Firm"] = df["Law Firm"].str.replace(", PO Bo", "")
		df["Law Firm"] = df["Law Firm"].str.replace(", P O Bo", "")

		# Sort by city
		df = df.sort_values(by=["City", "Website", "Name"], ignore_index=True)
		df = df.reset_index(drop=True)

		df = df[["Name", "City", "Law Firm", "Address", "Email", "Website"]].copy()

		df["DA Score"] = ""

		print(df.head())
		print(df.tail())
		print(len(df))

		#print(df[df["Website"].str.contains("health", na=False)]["Website"].unique().tolist())

		df.to_csv("ca-lawyers-with-email-only-final.csv", index=False)

	def final_processing12(self):
		df1 = pd.read_csv("ca-lawyers-with-email-only-final.csv", na_filter=False)
		df2 = pd.read_csv("ca-lawyers-with-website-only-final-cleaned-da-score-30.csv", na_filter=False)

		df1 = df1.astype(str)
		df2 = df2.astype(str)

		urls = df2["Website"].tolist()
		da_scores = df2["DA Score"].tolist()

		#print(len(urls))
		#print(len(da_scores))

		for i in range(len(urls)):
			df1.loc[df1["Website"] == urls[i], "DA Score"] = da_scores[i]

		df3 = df1[df1["DA Score"] != ""]
		df3 = df3.reset_index(drop=True)

		print(df3.head())
		print(df3.tail())
		print(len(df3))

		df = df2.append(df3, ignore_index=True)

		excluded_court = ["occourts.org", "sb-court.org", "lacourt.org", "lasuperiorcourt.org", "yubacourts.org", "mercedcourt.org", "eldoradocourt.org", "sanmateocourt.org", 
						  "sb-courts.org", "scscourt.org", "sbcourts.org", "santacruzcourt.org", "sonomacourt.org", "sjcourts.org", "suttercourts.com", "stanct.org", "nccn.net", 
						  "sftc.org", "fnf.com", "roll.com", "fd.org", "eero.com", "okta.com", "amat.com"]

		excluded_city = ["alamedacityattorney.org", "bakersfieldcity.us", "cityofberkeley.info", "city-attorney.com", "comptoncity.org", "cityofconcord.org",
						 "crescentcity.org", "culvercity.org", "dalycity.org", "elkgrovecity.org", "cityofgoleta.org", "cityofhawthorne.org", "surfcity-hb.org",
						 "cityoflancasterca.org", "cityoflivermore.net", "lacity.org", "atty.lacity.org", "cityview.com", "cityofmerced.org", "cityofnapa.org",
						 "oaklandcityattorney.org", "cityoforange.org", "cityofpaloalto.org", "cityofpasadena.net", "cityworksinc.org", "cityofpetaluma.org", 
						 "cityofredding.org", "redwoodcity.org", "cityofsacramento.org", "rivercitybank.com", "sfcityatty.org", "slocity.org", "cityofsanmateo.org",
						 "cityofsantamaria.org", "surfcity.net", "srcity.org", "cityofslt.us", "cityofvacaville.com", "cityofvallejo.net", "hacityventura.org", 
						 "cityofwestsacramento.org", "anaheim.net", "burlingame.org", "cupertino.org", "coh.org", "ladwp.com", "mindspring.com", "newark.org", 
						 "redondo.org", "sfha.org", "santa-ana.org", "simivalley.org", "toaks.org", "walnut-creek.org", "weho.org"]

		excluded_county = ["stancounty.com", "kerncounty.com", "theurbannestnorthcounty.com", "countyofcolusa.com", "countyofcolusa.org", "solanocounty.com", 
						   "solanocountylawyer.com", "mendocinocounty.org", "inyocounty.us", "lacountyda.org", "lacountypubdef.org", "madera-county.com", 
						   "maderacounty.com", "mariposacounty.org", "cc.cccounty.us", "dcss.cccounty.us", "pd.cccounty.us", "countyofmerced.com", "countyofnapa.org", 
						   "nevadacountycourts.com", "marincounty.org", "buttecounty.net", "countyofplumas.com", "saccounty.net", "pd.sbcounty.go", "countyofsb.org", 
						   "santacruzcounty.us", "sonoma-county.org", "mendocinocounty.com", "tularecountylaw.com", "sdcountytrusts.com", "trinitycounty.org", 
						   "countyofglenn.net", "ccm.yolocounty.org", "yolocounty.org", "ventura.org", "apalc.org", "pcwa.net", "kcera.org", "kcwa.com", "ocde.us", 
						   "smharbor.com", "fcoe.org", "cosb.us", "firstteam.com", "lafsbc.org", "lalawlibrary.org", "lasd.org", "metro.net", "yubawater.org", 
						   "juno.com", "stanct.org", "nccn.net", "choc.org", "lacera.com", "rcoe.us", "counties.org", "scoe.net", 
						   "gosbcta.com", "san.org", "sdapcd.org", "sdcwa.org", "sfha.org", "catholiccharitiesscc.org", "sccba.com", "mac.com", "habitatoc.org", 
						   "ocsd.org", "rcocdd.com", "coco.net.org"]

		excluded_health = ["alamedaalliance.org", "bayer.com", "pmhd.org", "color.com", "lyrahealth.com", "sutterhealth.org", "communitymedical.org", "brighthealthcare.com", 
						   "nanthealth.com", "pipelinehealth.us", "sidecarhealth.com", "northbay.org", "partnershiphp.org", "memorialcare.org", "gvhc.org", "healthcomp.com", 
						   "cigna.com", "commonspirit.org", "dignityhealth.org", "mymarinhealth.org", "landmarkhealth.org", "primehealthcare.com", "bestlife.com", "dentalxchange.com", 
						   "fphcare.com", "healthpeak.com", "mac.com", "nextgen.com", "onedigital.com", "providence.org", "apria.com", "agilonhealth.com", "scanhealthplan.com", "ahf.org", 
						   "healthmanagement.com", "lacare.org", "valleychildrens.org", "hioscar.com", "healthiq.com", "olehealth.org", "alamedahealthsystem.org", "elemenohealth.com", 
						   "kp.org", "phi.org", "eisenhowerhealth.org", "guardanthealth.com", "changehealthcare.com", "hnfs.com", "westernhealth.com", "amnhealthcare.com", "ayahealthcare.com", 
						   "fhcsd.org", "lamaestra.org", "medimpact.com", "rchsd.org", "sharp.com", "castlighthealth.com", "forhims.com", "ginger.io", "komodohealth.com", "omadahealth.com", 
						   "quantumleaphealth.org", "palomarhealth.org", "evidation.com", "tenethealth.com", "lifegen.net", "cencalhealth.org", "sbch.org", "ehealth.com", "cmhshealth.org", 
						   "fhcn.org", "johnmuirhealth.com", "emanatehealth.org", "borregohealth.org", "lyrahealth.com", "sutterhealth.org", "brighthealthcare.com", "nanthealth.com", 
						   "pipelinehealth.us", "sidecarhealth.com", "healthcomp.com", "dignityhealth.org", "mymarinhealth.org", "landmarkhealth.org", "primehealthcare.com", "11health.com", 
						   "healthpeak.com", "agilonhealth.com", "scanhealthplan.com", "health-law.com", "healthmanagement.com", "healthiq.com", "olehealth.org", "alamedahealthsystem.org", 
						   "elemenohealth.com", "eisenhowerhealth.org", "adverahealth.com", "guardanthealth.com", "changehealthcare.com", "westernhealth.com", "amnhealthcare.com", "ayahealthcare.com", 
						   "castlighthealth.com", "komodohealth.com", "omadahealth.com", "quantumleaphealth.org", "palomarhealth.org", "tenethealth.com", "cencalhealth.org", "ehealth.com", "cmhshealth.org", 
						   "johnmuirhealth.com", "emanatehealth.org", "evolenthealth.com"]

		excluded_medical = ["coh.org", "novartis.com", "communitymedical.org", "samc.com", "mymarinhealth.org", "inarimedical.com", "varian.com", "cshs.org", "prospectmedical.com", 
							"talisbio.com", "kp.org", "recormedical.com", "eisenhowerhealth.org", "appliedmedical.com", "cmadocs.org", "medpro.com", "revamedical.com", "onemedical.com", 
							"reflexion.com", "shockwavemedical.com", "us.medical.canon", "avitamedical.com", "communitymedical.org", "inarimedical.com", "prospectmedical.com", "recormedical.com", 
							"appliedmedical.com", "revamedical.com", "onemedical.com", "shockwavemedical.com", "us.medical.canon", "avitamedical.com"]

		excluded_bio = ["cariboubio.com", "pivotbio.com", "nantworks.com", "novartis.com", "arcusbio.com", "bio-rad.com", "powercrunch.com", "pumabiotechnology.com", "pacb.com", "talisbio.com", 
						"atarabio.com", "bmrn.com", "bridgebio.com", "coherus.com", "twistbioscience.com", "3ds.com", "bioatla.com", "biosplice.com", "neurocrine.com", "phasebio.com", "gatesfoundation.org", 
						"vir.bio", "kronosbio.com", "ngmbio.com", "sana.com", "sutrobio.com", "avidbio.com", "arcusbio.com", "bio-rad.com", "pumabiotechnology.com", "talisbio.com", "atarabio.com", "bridgebio.com", 
						"twistbioscience.com", "bioatla.com", "biosplice.com", "phasebio.com", "us.ajibio-pharma.com", "vir.bio", "kronosbio.com", "missionbio.com", "ngmbio.com", "sutrobio.com", "avidbio.com", 
						"veeva.com", "vir.bio", "kronosbio.com", "lionsgate.com", "missionbio.com", "ngmbio.com", "sutrobio.com", "avidbio.com"]

		excluded_entertainment = ["dmg-entertainment.com", "imagine-entertainment.com", "dcentertainment.com", "ep.com", "legendary.com", "stxentertainment.com", "mgae.com", "prodigy.net", 
								  "sonymusic.com", "spe.sony.com", "spp11.msmail.spe.sony.com", "konami.com", "firstent.org", "aegworldwide.com", "condenast.com", "es.tv", "fox.com", 
								  "halcyonstudios.tv", "interplay.com", "marvista.net", "sony.com", "vreg.com", "webtoon.com", "bentoboxent.com", "lionsgate.com", "starz.com", "cbs.com", 
								  "lightyear.com", "nbcuni.com", "electricentertainment.com", "c3entertainment.com", "dcentertainment.com", "stxentertainment.com", "electricentertainment.com"]

		excluded_studio = ["bronstudios.com", "mgm.com", "abc.com", "amazon.com", "amazonstudios.com", "technicolor.com", "pixar.com", "boatrocker.com", "cbs.com", "es.tv", "itv.com", 
						   "pilgrimstudios.com", "mbww.com", "nbcuni.com", "studio71us.com", "halcyonstudios.tv", "x2studios.com"]

		excluded_media = ["fandango.com", "bydeluxe.com", "turner.com", "viacom.com", "warnermedia.com", "salem.cc", "salemmedia.com", "juno.com", "nfl.com", "participant.com", "propelmedia.com", 
						  "boatrocker.com", "es.tv", "jukinmedia.com", "madisonwellsmedia.com", "walden.com", "medianewsgroup.com", "pandora.com", "encompass.tv", "newmediarights.org", "wikimedia.org", 
						  "cox.net", "mbww.com", "redbull.com", "skydance.com", "crownmedia.com", "intermedia.net", "nbcuni.com", "visionmedia.com", "pilgrimmediagroup.com", "voxmedia.com", 
						  "salemmedia.com", "groupninemedia.com", "mediaone.net", "shedmedia.com", "jukinmedia.com", "newmediarights.org", "industrial-media.com"]

		excluded_music = ["afm.org", "peermusic.com", "sonymusic.com", "fender.com", "globalmusicrights.com", "umusic.com", "warnerchappell.com", "warnerchappellpm.com", "manifesto.com", "extrememusic.com", 
						  "musicreports.com", "umusic.com"]

		excluded_tv = ["spe.sony.com", "directv.com", "betfair.com", "foxtv.com", "nextvr.com", "itv.com", "amctv.com", "amptp.org", "nbcuni.com", "cwtv.com", "telepixtv.com", "directv.com", "revry.tv", 
					   "belmontvillage.com", "es.tv", "foxtv.com", "halcyonstudios.tv", "nextvr.com", "itv.com", "encompass.tv", "ymcaeastvalley.org", "tatari.tv", "tubi.tv", 
					   "twitch.tv", "amctv.com", "netvision.net.il", "cwtv.com", "telepixtv.com", "revry.tv", "capphysicians.com", "belmontvillage.com", "bnymellon.com", "es.tv", "foxtv.com", 
					   "halcyonstudios.tv", "nextvr.com", "itv.com", "thezenith.com", "encompass.tv", "ymcaeastvalley.org", "lighthouseglobal.com", "tatari.tv", "amctv.com", "netvision.net.il"]

		excluded_film = ["ubisoft.com", "ifta-online.org", "miramax.com", "lionsgate.com", "openroadfilms.com", "filmrise.com", "openroadfilms.com", "a24films.com", "filmrise.com"]

		excluded_bank = ["bankspower.com", "truist.com", "tcbk.com", "fremontbank.com", "pacwest.com", "camoves.com", "mechanicsbank.com", "ppbi.com", "unionbank.com", "bankofhope.com", "bofa.com", "citi.com", 
						 "firstrepublic.com", "hanmi.com", "jpmorgan.com", "us.mufg.jp", "usbank.com", "svb.com", "cbbank.com", "comerica.com", "bankofthewest.com", "cit.com", "sacramentofoodbank.org", "softbank.com", 
						 "axosbank.com", "bnymellon.com", "fhlbsf.com", "lendingclub.com", "sf.frb.org", "bostonprivate.com", "db.com", "exchangebank.com", "jpmchase.com", "cathaybank.com", "bankspower.com", "cbbank.com", 
						 "bofifederalbank.com", "fremontbank.com", "mechanicsbank.com", "unionbank.com", "bankofthewest.com", "bankofhope.com", "usbank.com", "sacramentofoodbank.org", "axosbank.com", "exchangebank.com", 
						 "nextgen.com", "unionbank.com", "bankofthewest.com", "bankofhope.com", "fb.com", "sacramentofoodbank.org", "axosbank.com", "lockton.com", "exchangebank.com"]

		excluded_financial = ["afncorp.com", "riafinancial.com", "civicfs.com", "smartfinancial.com", "broadridge.com", "athene.com", "cetera.com", "principal.com", "amtrustgroup.com", "fnf.com", "lpl.com", "finra.org", 
							  "hanmi.com", "prudential.com", "ubs.com", "westlakefinancial.com", "etrade.com", "ladderlife.com", "coop.org", "mwfinc.com", "bolt.com", "truelinkfinancial.com", "bills.com", "svb.com", 
							  "homebridge.com", "pennymac.com", "riafinancial.com", "smartfinancial.com", "westlakefinancial.com", "truelinkfinancial.com"]

		excluded_insurance = ["doma.com", "marshmma.com", "pacificspecialty.com", "cna.com", "selective.com", "libertymutual.com", "travelers.com", "hanover.com", "newyorklife.com", "bestlife.com", "lockton.com", "aig.com", 
							  "archinsurance.com", "chubb.com", "ctt.com", "farmersinsurance.com", "heffins.com", "alliant.com", "thezenith.com", "allstate.com", "amfam.com", "fnf.com", "wawanesa.com", "bhhc.com", "epicbrokers.com", 
							  "everestre.com", "metlife.com", "nationwide.com", "ortc.com", "willis.com", "zurichna.com", "afgroup.com", "twc.com", "insurancefornonprofits.org", "higginbotham.net", "axaxl.com", "csaa.com", "pmigroup.com", 
							  "copperpoint.com", "markel.com", "mercuryinsurance.com", "archinsurance.com", "farmersinsurance.com", "cseinsurance.com", "insurancefornonprofits.org"]

		# Remove lawyers with unwanted emails
		unwanted = [".edu", ".mil", ".gov", "ca.us", "me.com", "1cbank", "cs.com", "qq.com", "gmx.us", "bit.ly", "cox.net", "hpe.com", "att.net", "dga.org", "aol.com", "uhc.com", "mac.com", "mgm.com", "cnb.com", "msn.com", 
					"wmg.com", "fox.com", "gmx.com", "cbre.com", "psln.com", "zoho.com", "sfhp.org", "khov.com", "ah4r.com", "qnet.com", "divx.com", "scif.com", "mail.com", "iehp.org", "live.com", "pmhd.org", "gmail.com", 
					"frbsf.org", "yahoo.com", "bayer.com", "zorro.com", "intel.com", "epson.com", "apple.com", "mavtv.com", "acphd.org", "sonic.net", "tesla.com", "oscars.org", "disney.com", "edf-re.com", "waltdisney", 
					"nvidia.com", "google.com", "vna.health", "yandex.com", "cydcor.com", "abbott.com", "tubitv.com", "icumed.com", "cbbank.com", "davita.com", "icloud.com", "verizon.com", "outlook.com", "firstam.com", 
					"hotbench.tv", "chevron.com", "udacity.com", "calfund.org", "anaheim.net", "comcast.net", "rivcoda.org", "8minute.com", "agilent.com", "hotmail.com", "phcdocs.org", "samsung.com", "tinyurl.com", "gvhomes.com", 
					"legionm.com", "altamed.org", "verizon.net", "pacbell.net", "softbank.com", "fandango.com", "netscape.net", "linkedin.com", "facebook.com", "deloitte.com", "qualcomm.com", "teamwass.com", "ovationtv.com", 
					"ingrooves.com", "coveredca.com", "dollskill.com", "peacocktv.com", "bankofamerica", "sbcglobal.net", "lucasfilm.com", "wmeagency.com", "fremantle.com", "paramount.com", "earthlink.net", "viacomcbs.com", 
					"servicenow.com", "bridgebank.com", "livenation.com", "mindspring.com", "salesforce.com", "montecito.bank", "smartvoter.org", "roadrunner.com", "protonmail.com", "aidshealth.org", "rocketmail.com", 
					"silvergate.com", "brightview.com", "warnerbros.com", "studio71us.com", "uclahealth.org", "wellsfargo.com", "virginorbit.com", "progressive.com", "denovahomes.com", "bronstudios.com", "homebaseccc.org", 
					"hingehealth.com", "checkerspot.com", "theagencyre.com", "aegpresents.com", "kernmedical.com", "playstation.com", "nbcuniversal.com", "unitedtalent.com", "landseahomes.com", "carbonhealth.com", "sonypictures.com", 
					"eastwestbank.com", "borregohealth.org", "scrippshealth.org", "universalmusic.com", "commercecasino.com", "paradigmagency.com", "discoverylandco.com", "relativityspace.com", "adventisthealth.org", "molinahealthcare.com", 
					"mrcentertainment.com", "createmusicgroup.com", "dmg-entertainment.com", "firstfoundationinc.com", "stanfordhealthcare.org", "thecheesecakefactory.com", "entertainmentstudios.com", "discoverybehavioralhealth.com", 
					"lakeshorelearning.com", "amat.com", "laird.com", "fbmsales.com"]

		df = df[~df["Website"].str.contains("|".join(excluded_court))]
		df = df[~df["Website"].str.contains("|".join(excluded_city))]
		df = df[~df["Website"].str.contains("|".join(excluded_county))]
		df = df[~df["Website"].str.contains("|".join(unwanted))]

		pattern = r"^(\D*)\d"
		df["Law Firm"] = df["Address"].str.extract(pattern, expand=False)
		df["Law Firm"] = df["Law Firm"].str[:-2]
		df["Law Firm"] = df["Law Firm"].str.replace(", PO Bo", "")
		df["Law Firm"] = df["Law Firm"].str.replace(", P O Bo", "")

		# Sort by city
		df = df.sort_values(by=["City", "Website", "Name"], ignore_index=True)
		df = df.reset_index(drop=True)

		df = df[["Name", "City", "Law Firm", "Address", "Email", "Website", "DA Score"]].copy()

		print(df.head())
		print(df.tail())
		print(len(df))

		df.to_csv("ca-lawyers-with-email-and-website-da-score-30-batch-1.csv", index=False)

		df4 = df1[df1["DA Score"] == ""]

		df4 = df4[~df4["Website"].str.contains("|".join(excluded_court))]
		df4 = df4[~df4["Website"].str.contains("|".join(excluded_city))]
		df4 = df4[~df4["Website"].str.contains("|".join(excluded_county))]
		df4 = df4[~df4["Website"].str.contains("|".join(unwanted))]

		# Sort by city
		df4 = df4.sort_values(by=["City", "Website", "Name"], ignore_index=True)
		df4 = df4.reset_index(drop=True)

		df4 = df4[["Name", "City", "Law Firm", "Address", "Email", "Website", "DA Score"]].copy()

		print(df4.head())
		print(df4.tail())
		print(len(df4))

		df4.to_csv("ca-lawyers-with-email-and-website-da-score-unknown.csv", index=False)

	def final_processing13(self):
		df1 = pd.read_csv("ca-lawyers-with-email-and-website-da-score-unknown.csv", na_filter=False)
		df2 = pd.read_csv("ca-law-firms-with-website-master.csv", na_filter=False)
		
		df1 = df1.astype(str)
		df2 = df2.astype(str)

		urls = df2["Website"].tolist()
		da_scores = df2["DA Score"].tolist()

		#print(len(urls))
		#print(len(da_scores))

		for i in range(len(urls)):
			df1.loc[df1["Website"] == urls[i], "DA Score"] = da_scores[i]

		df3 = df1[df1["DA Score"] != ""]

		print("Before filtering")
		print(df3.head())
		print(df3.tail())
		print(len(df3))

		df3["DA Score"] = df3["DA Score"].astype(int)

		df3 = df3[df3["DA Score"] >= 30]

		df3 = df3.sort_values(by=["City", "Website", "Name"], ignore_index=True)
		df3 = df3.reset_index(drop=True)

		print("After filtering")
		print(df3.head())
		print(df3.tail())
		print(len(df3))

		df3.to_csv("ca-lawyers-with-email-and-website-da-score-30-batch-2.csv", index=False)

		df4 = df1[df1["DA Score"] == ""]

		df4 = df4.sort_values(by=["City", "Website", "Name"], ignore_index=True)
		df4 = df4.reset_index(drop=True)

		print(df4.head())
		print(df4.tail())
		print(len(df4))

		df4.to_csv("ca-lawyers-with-email-and-website-da-score-unknown.csv", index=False)

	def final_processing14(self):
		df1 = pd.read_csv("ca-lawyers-with-email-and-website-da-score-30-batch-1.csv", na_filter=False)
		df2 = pd.read_csv("ca-lawyers-with-email-and-website-da-score-30-batch-2.csv", na_filter=False)
		df3 = pd.read_csv("ca-lawyers-with-email-and-website-da-score-30-batch-3.csv", na_filter=False)

		df = pd.concat([df1, df2, df3], axis=0)

		df = df.sort_values(by=["City", "Website", "Name"], ignore_index=True)
		df = df.reset_index(drop=True)

		print(df.head())
		print(df.tail())
		print(len(df))

		excluded_court = ["occourts.org", "sb-court.org", "lacourt.org", "lasuperiorcourt.org", "yubacourts.org", "mercedcourt.org", "eldoradocourt.org", "sanmateocourt.org", 
						  "sb-courts.org", "scscourt.org", "sbcourts.org", "santacruzcourt.org", "sonomacourt.org", "sjcourts.org", "suttercourts.com", "stanct.org", "nccn.net", 
						  "sftc.org", "fnf.com", "roll.com", "fd.org", "eero.com", "okta.com", "amat.com"]

		excluded_city = ["alamedacityattorney.org", "bakersfieldcity.us", "cityofberkeley.info", "city-attorney.com", "comptoncity.org", "cityofconcord.org",
						 "crescentcity.org", "culvercity.org", "dalycity.org", "elkgrovecity.org", "cityofgoleta.org", "cityofhawthorne.org", "surfcity-hb.org",
						 "cityoflancasterca.org", "cityoflivermore.net", "lacity.org", "atty.lacity.org", "cityview.com", "cityofmerced.org", "cityofnapa.org",
						 "oaklandcityattorney.org", "cityoforange.org", "cityofpaloalto.org", "cityofpasadena.net", "cityworksinc.org", "cityofpetaluma.org", 
						 "cityofredding.org", "redwoodcity.org", "cityofsacramento.org", "rivercitybank.com", "sfcityatty.org", "slocity.org", "cityofsanmateo.org",
						 "cityofsantamaria.org", "surfcity.net", "srcity.org", "cityofslt.us", "cityofvacaville.com", "cityofvallejo.net", "hacityventura.org", 
						 "cityofwestsacramento.org", "anaheim.net", "burlingame.org", "cupertino.org", "coh.org", "ladwp.com", "mindspring.com", "newark.org", 
						 "redondo.org", "sfha.org", "santa-ana.org", "simivalley.org", "toaks.org", "walnut-creek.org", "weho.org"]

		excluded_county = ["stancounty.com", "kerncounty.com", "theurbannestnorthcounty.com", "countyofcolusa.com", "countyofcolusa.org", "solanocounty.com", 
						   "solanocountylawyer.com", "mendocinocounty.org", "inyocounty.us", "lacountyda.org", "lacountypubdef.org", "madera-county.com", 
						   "maderacounty.com", "mariposacounty.org", "cc.cccounty.us", "dcss.cccounty.us", "pd.cccounty.us", "countyofmerced.com", "countyofnapa.org", 
						   "nevadacountycourts.com", "marincounty.org", "buttecounty.net", "countyofplumas.com", "saccounty.net", "pd.sbcounty.go", "countyofsb.org", 
						   "santacruzcounty.us", "sonoma-county.org", "mendocinocounty.com", "tularecountylaw.com", "sdcountytrusts.com", "trinitycounty.org", 
						   "countyofglenn.net", "ccm.yolocounty.org", "yolocounty.org", "ventura.org", "apalc.org", "pcwa.net", "kcera.org", "kcwa.com", "ocde.us", 
						   "smharbor.com", "fcoe.org", "cosb.us", "firstteam.com", "lafsbc.org", "lalawlibrary.org", "lasd.org", "metro.net", "yubawater.org", 
						   "juno.com", "stanct.org", "nccn.net", "choc.org", "lacera.com", "rcoe.us", "counties.org", "scoe.net", 
						   "gosbcta.com", "san.org", "sdapcd.org", "sdcwa.org", "sfha.org", "catholiccharitiesscc.org", "sccba.com", "mac.com", "habitatoc.org", 
						   "ocsd.org", "rcocdd.com", "coco.net.org"]

		excluded_health = ["alamedaalliance.org", "bayer.com", "pmhd.org", "color.com", "lyrahealth.com", "sutterhealth.org", "communitymedical.org", "brighthealthcare.com", 
						   "nanthealth.com", "pipelinehealth.us", "sidecarhealth.com", "northbay.org", "partnershiphp.org", "memorialcare.org", "gvhc.org", "healthcomp.com", 
						   "cigna.com", "commonspirit.org", "dignityhealth.org", "mymarinhealth.org", "landmarkhealth.org", "primehealthcare.com", "bestlife.com", "dentalxchange.com", 
						   "fphcare.com", "healthpeak.com", "mac.com", "nextgen.com", "onedigital.com", "providence.org", "apria.com", "agilonhealth.com", "scanhealthplan.com", "ahf.org", 
						   "healthmanagement.com", "lacare.org", "valleychildrens.org", "hioscar.com", "healthiq.com", "olehealth.org", "alamedahealthsystem.org", "elemenohealth.com", 
						   "kp.org", "phi.org", "eisenhowerhealth.org", "guardanthealth.com", "changehealthcare.com", "hnfs.com", "westernhealth.com", "amnhealthcare.com", "ayahealthcare.com", 
						   "fhcsd.org", "lamaestra.org", "medimpact.com", "rchsd.org", "sharp.com", "castlighthealth.com", "forhims.com", "ginger.io", "komodohealth.com", "omadahealth.com", 
						   "quantumleaphealth.org", "palomarhealth.org", "evidation.com", "tenethealth.com", "lifegen.net", "cencalhealth.org", "sbch.org", "ehealth.com", "cmhshealth.org", 
						   "fhcn.org", "johnmuirhealth.com", "emanatehealth.org", "borregohealth.org", "lyrahealth.com", "sutterhealth.org", "brighthealthcare.com", "nanthealth.com", 
						   "pipelinehealth.us", "sidecarhealth.com", "healthcomp.com", "dignityhealth.org", "mymarinhealth.org", "landmarkhealth.org", "primehealthcare.com", "11health.com", 
						   "healthpeak.com", "agilonhealth.com", "scanhealthplan.com", "health-law.com", "healthmanagement.com", "healthiq.com", "olehealth.org", "alamedahealthsystem.org", 
						   "elemenohealth.com", "eisenhowerhealth.org", "adverahealth.com", "guardanthealth.com", "changehealthcare.com", "westernhealth.com", "amnhealthcare.com", "ayahealthcare.com", 
						   "castlighthealth.com", "komodohealth.com", "omadahealth.com", "quantumleaphealth.org", "palomarhealth.org", "tenethealth.com", "cencalhealth.org", "ehealth.com", "cmhshealth.org", 
						   "johnmuirhealth.com", "emanatehealth.org", "evolenthealth.com"]

		excluded_medical = ["coh.org", "novartis.com", "communitymedical.org", "samc.com", "mymarinhealth.org", "inarimedical.com", "varian.com", "cshs.org", "prospectmedical.com", 
							"talisbio.com", "kp.org", "recormedical.com", "eisenhowerhealth.org", "appliedmedical.com", "cmadocs.org", "medpro.com", "revamedical.com", "onemedical.com", 
							"reflexion.com", "shockwavemedical.com", "us.medical.canon", "avitamedical.com", "communitymedical.org", "inarimedical.com", "prospectmedical.com", "recormedical.com", 
							"appliedmedical.com", "revamedical.com", "onemedical.com", "shockwavemedical.com", "us.medical.canon", "avitamedical.com"]

		excluded_bio = ["cariboubio.com", "pivotbio.com", "nantworks.com", "novartis.com", "arcusbio.com", "bio-rad.com", "powercrunch.com", "pumabiotechnology.com", "pacb.com", "talisbio.com", 
						"atarabio.com", "bmrn.com", "bridgebio.com", "coherus.com", "twistbioscience.com", "3ds.com", "bioatla.com", "biosplice.com", "neurocrine.com", "phasebio.com", "gatesfoundation.org", 
						"vir.bio", "kronosbio.com", "ngmbio.com", "sana.com", "sutrobio.com", "avidbio.com", "arcusbio.com", "bio-rad.com", "pumabiotechnology.com", "talisbio.com", "atarabio.com", "bridgebio.com", 
						"twistbioscience.com", "bioatla.com", "biosplice.com", "phasebio.com", "us.ajibio-pharma.com", "vir.bio", "kronosbio.com", "missionbio.com", "ngmbio.com", "sutrobio.com", "avidbio.com", 
						"veeva.com", "vir.bio", "kronosbio.com", "lionsgate.com", "missionbio.com", "ngmbio.com", "sutrobio.com", "avidbio.com"]

		excluded_entertainment = ["dmg-entertainment.com", "imagine-entertainment.com", "dcentertainment.com", "ep.com", "legendary.com", "stxentertainment.com", "mgae.com", "prodigy.net", 
								  "sonymusic.com", "spe.sony.com", "spp11.msmail.spe.sony.com", "konami.com", "firstent.org", "aegworldwide.com", "condenast.com", "es.tv", "fox.com", 
								  "halcyonstudios.tv", "interplay.com", "marvista.net", "sony.com", "vreg.com", "webtoon.com", "bentoboxent.com", "lionsgate.com", "starz.com", "cbs.com", 
								  "lightyear.com", "nbcuni.com", "electricentertainment.com", "c3entertainment.com", "dcentertainment.com", "stxentertainment.com", "electricentertainment.com"]

		excluded_studio = ["bronstudios.com", "mgm.com", "abc.com", "amazon.com", "amazonstudios.com", "technicolor.com", "pixar.com", "boatrocker.com", "cbs.com", "es.tv", "itv.com", 
						   "pilgrimstudios.com", "mbww.com", "nbcuni.com", "studio71us.com", "halcyonstudios.tv", "x2studios.com"]

		excluded_media = ["fandango.com", "bydeluxe.com", "turner.com", "viacom.com", "warnermedia.com", "salem.cc", "salemmedia.com", "juno.com", "nfl.com", "participant.com", "propelmedia.com", 
						  "boatrocker.com", "es.tv", "jukinmedia.com", "madisonwellsmedia.com", "walden.com", "medianewsgroup.com", "pandora.com", "encompass.tv", "newmediarights.org", "wikimedia.org", 
						  "cox.net", "mbww.com", "redbull.com", "skydance.com", "crownmedia.com", "intermedia.net", "nbcuni.com", "visionmedia.com", "pilgrimmediagroup.com", "voxmedia.com", 
						  "salemmedia.com", "groupninemedia.com", "mediaone.net", "shedmedia.com", "jukinmedia.com", "newmediarights.org", "industrial-media.com"]

		excluded_music = ["afm.org", "peermusic.com", "sonymusic.com", "fender.com", "globalmusicrights.com", "umusic.com", "warnerchappell.com", "warnerchappellpm.com", "manifesto.com", "extrememusic.com", 
						  "musicreports.com", "umusic.com"]

		excluded_tv = ["spe.sony.com", "directv.com", "betfair.com", "foxtv.com", "nextvr.com", "itv.com", "amctv.com", "amptp.org", "nbcuni.com", "cwtv.com", "telepixtv.com", "directv.com", "revry.tv", 
					   "belmontvillage.com", "es.tv", "foxtv.com", "halcyonstudios.tv", "nextvr.com", "itv.com", "encompass.tv", "ymcaeastvalley.org", "tatari.tv", "tubi.tv", 
					   "twitch.tv", "amctv.com", "netvision.net.il", "cwtv.com", "telepixtv.com", "revry.tv", "capphysicians.com", "belmontvillage.com", "bnymellon.com", "es.tv", "foxtv.com", 
					   "halcyonstudios.tv", "nextvr.com", "itv.com", "thezenith.com", "encompass.tv", "ymcaeastvalley.org", "lighthouseglobal.com", "tatari.tv", "amctv.com", "netvision.net.il"]

		excluded_film = ["ubisoft.com", "ifta-online.org", "miramax.com", "lionsgate.com", "openroadfilms.com", "filmrise.com", "openroadfilms.com", "a24films.com", "filmrise.com"]

		excluded_bank = ["bankspower.com", "truist.com", "tcbk.com", "fremontbank.com", "pacwest.com", "camoves.com", "mechanicsbank.com", "ppbi.com", "unionbank.com", "bankofhope.com", "bofa.com", "citi.com", 
						 "firstrepublic.com", "hanmi.com", "jpmorgan.com", "us.mufg.jp", "usbank.com", "svb.com", "cbbank.com", "comerica.com", "bankofthewest.com", "cit.com", "sacramentofoodbank.org", "softbank.com", 
						 "axosbank.com", "bnymellon.com", "fhlbsf.com", "lendingclub.com", "sf.frb.org", "bostonprivate.com", "db.com", "exchangebank.com", "jpmchase.com", "cathaybank.com", "bankspower.com", "cbbank.com", 
						 "bofifederalbank.com", "fremontbank.com", "mechanicsbank.com", "unionbank.com", "bankofthewest.com", "bankofhope.com", "usbank.com", "sacramentofoodbank.org", "axosbank.com", "exchangebank.com", 
						 "nextgen.com", "unionbank.com", "bankofthewest.com", "bankofhope.com", "fb.com", "sacramentofoodbank.org", "axosbank.com", "lockton.com", "exchangebank.com"]

		excluded_financial = ["afncorp.com", "riafinancial.com", "civicfs.com", "smartfinancial.com", "broadridge.com", "athene.com", "cetera.com", "principal.com", "amtrustgroup.com", "fnf.com", "lpl.com", "finra.org", 
							  "hanmi.com", "prudential.com", "ubs.com", "westlakefinancial.com", "etrade.com", "ladderlife.com", "coop.org", "mwfinc.com", "bolt.com", "truelinkfinancial.com", "bills.com", "svb.com", 
							  "homebridge.com", "pennymac.com", "riafinancial.com", "smartfinancial.com", "westlakefinancial.com", "truelinkfinancial.com", "creditkarma.com"]

		excluded_insurance = ["doma.com", "marshmma.com", "pacificspecialty.com", "cna.com", "selective.com", "libertymutual.com", "travelers.com", "hanover.com", "newyorklife.com", "bestlife.com", "lockton.com", "aig.com", 
							  "archinsurance.com", "chubb.com", "ctt.com", "farmersinsurance.com", "heffins.com", "alliant.com", "thezenith.com", "allstate.com", "amfam.com", "fnf.com", "wawanesa.com", "bhhc.com", "epicbrokers.com", 
							  "everestre.com", "metlife.com", "nationwide.com", "ortc.com", "willis.com", "zurichna.com", "afgroup.com", "twc.com", "insurancefornonprofits.org", "higginbotham.net", "axaxl.com", "csaa.com", "pmigroup.com", 
							  "copperpoint.com", "markel.com", "mercuryinsurance.com", "archinsurance.com", "farmersinsurance.com", "cseinsurance.com", "insurancefornonprofits.org"]

		# Remove lawyers with unwanted emails
		unwanted = [".edu", ".mil", ".gov", "ca.us", "me.com", "1cbank", "cs.com", "qq.com", "gmx.us", "bit.ly", "cox.net", "hpe.com", "att.net", "dga.org", "aol.com", "uhc.com", "mac.com", "mgm.com", "cnb.com", "msn.com", 
					"wmg.com", "fox.com", "gmx.com", "cbre.com", "psln.com", "zoho.com", "sfhp.org", "khov.com", "ah4r.com", "qnet.com", "divx.com", "scif.com", "mail.com", "iehp.org", "live.com", "pmhd.org", "gmail.com", 
					"frbsf.org", "yahoo.com", "bayer.com", "zorro.com", "intel.com", "epson.com", "apple.com", "mavtv.com", "acphd.org", "sonic.net", "tesla.com", "oscars.org", "disney.com", "edf-re.com", "waltdisney", 
					"nvidia.com", "google.com", "vna.health", "yandex.com", "cydcor.com", "abbott.com", "tubitv.com", "icumed.com", "cbbank.com", "davita.com", "icloud.com", "verizon.com", "outlook.com", "firstam.com", 
					"hotbench.tv", "chevron.com", "udacity.com", "calfund.org", "anaheim.net", "comcast.net", "rivcoda.org", "8minute.com", "agilent.com", "hotmail.com", "phcdocs.org", "samsung.com", "tinyurl.com", "gvhomes.com", 
					"legionm.com", "altamed.org", "verizon.net", "pacbell.net", "softbank.com", "fandango.com", "netscape.net", "linkedin.com", "facebook.com", "deloitte.com", "qualcomm.com", "teamwass.com", "ovationtv.com", 
					"ingrooves.com", "coveredca.com", "dollskill.com", "peacocktv.com", "bankofamerica", "sbcglobal.net", "lucasfilm.com", "wmeagency.com", "fremantle.com", "paramount.com", "earthlink.net", "viacomcbs.com", 
					"servicenow.com", "bridgebank.com", "livenation.com", "mindspring.com", "salesforce.com", "montecito.bank", "smartvoter.org", "roadrunner.com", "protonmail.com", "aidshealth.org", "rocketmail.com", 
					"silvergate.com", "brightview.com", "warnerbros.com", "studio71us.com", "uclahealth.org", "wellsfargo.com", "virginorbit.com", "progressive.com", "denovahomes.com", "bronstudios.com", "homebaseccc.org", 
					"hingehealth.com", "checkerspot.com", "theagencyre.com", "aegpresents.com", "kernmedical.com", "playstation.com", "nbcuniversal.com", "unitedtalent.com", "landseahomes.com", "carbonhealth.com", "sonypictures.com", 
					"eastwestbank.com", "borregohealth.org", "scrippshealth.org", "universalmusic.com", "commercecasino.com", "paradigmagency.com", "discoverylandco.com", "relativityspace.com", "adventisthealth.org", "molinahealthcare.com", 
					"mrcentertainment.com", "createmusicgroup.com", "dmg-entertainment.com", "firstfoundationinc.com", "stanfordhealthcare.org", "thecheesecakefactory.com", "entertainmentstudios.com", "discoverybehavioralhealth.com", 
					"lakeshorelearning.com", "amat.com", "laird.com", "fbmsales.com"]

		df = df[~df["Website"].str.contains("|".join(excluded_court + excluded_city + excluded_county + excluded_health + excluded_medical + excluded_bio))]
		df = df[~df["Website"].str.contains("|".join(excluded_entertainment + excluded_studio + excluded_media + excluded_music + excluded_tv + excluded_film))]
		df = df[~df["Website"].str.contains("|".join(excluded_bank + excluded_financial + excluded_insurance + unwanted))]

		df = df.sort_values(by=["City", "Website", "Name"], ignore_index=True)
		df = df.reset_index(drop=True)

		print(df.head())
		print(df.tail())
		print(len(df))

		df.to_csv("ca-lawyers-with-email-and-website-da-score-30-final-list.csv", index=False)

	def random_processing1(self):
		unwanted1 = ["bankofamerica", "waltdisney", "progressive.com", "discoverybehavioralhealth.com", "molinahealthcare.com", "sonypictures.com", "mrcentertainment.com", 
					"mavtv.com", "legionm.com", "livenation.com", "warnerbros.com", "playstation.com", "thecheesecakefactory.com", "entertainmentstudios.com", "createmusicgroup.com", 
					"relativityspace.com", "viacomcbs.com", "wellsfargo.com", "virginorbit.com", "aegpresents.com", "paramount.com", "roadrunner.com", "brightview.com", "dollskill.com", 
					"checkerspot.com", "rocketmail.com", "protonmail.com", "earthlink.net", "sbcglobal.net", "deloitte.com", "samsung.com", "salesforce.com", "facebook.com", "linkedin.com", 
					"calfund.org", "tinyurl.com", "firstfoundationinc.com", "bridgebank.com", "frbsf.org", "montecito.bank", "eastwestbank.com", "silvergate.com", "1cbank", "kernmedical.com", 
					"stanfordhealthcare.org", "phcdocs.org", "icumed.com", "scrippshealth.org", "verizon.net", "verizon.com", "comcast.net", "hotmail.com", "outlook.com", "udacity.com", 
					"chevron.com", "firstam.com", "agilent.com", "8minute.com", "carbonhealth.com", "hingehealth.com", "altamed.org", "davita.com", "aidshealth.org", "uclahealth.org", "iehp.org", 
					"adventisthealth.org", "coveredca.com", "commercecasino.com", "abbott.com", "hotbench.tv", "teamwass.com", "viacomcbs.com", "fremantle.com", "pacbell.net", "disney.com", 
					"cydcor.com", "yandex.com", "google.com", "icloud.com", "epson.com", "tesla.com", "yahoo.com", "gmail.com", "smartvoter.org", "rivcoda.org", "ovationtv.com", "tubitv.com", 
					"intel.com", "nvidia.com", "servicenow.com", "qualcomm.com", "nbcuniversal.com", "peacocktv.com", "lucasfilm.com", "ah4r.com", "denovahomes.com", "khov.com", "gvhomes.com", 
					"landseahomes.com", "homebaseccc.org", "universalmusic.com", "ingrooves.com", "sonic.net", "apple.com", "vna.health", "acphd.org", "sfhp.org", "cbre.com", "zoho.com", "qnet.com", 
					"scif.com", "mail.com", "uhc.com", "wmg.com", "dga.org", "cnb.com", "msn.com", "att.net", "aol.com", "gmx.com", "hpe.com", "qq.com", "me.com", "cs.com", "gmx.us", "bit.ly", "ca.us", 
					".edu", ".gov", ".mil"]

		unwanted2 = [".edu", ".gov", ".mil", "ca.us", "1cbank", "qq.com", "me.com", "cs.com", "gmx.us", "bit.ly", "uhc.com", "wmg.com", "dga.org", "cnb.com", "msn.com", "att.net", "aol.com", "mac.com", "mgm.com", "cox.net", "zorro.com", 
					"gmx.com", "hpe.com", "live.com", "divx.com", "iehp.org", "ah4r.com", "khov.com", "sfhp.org", "cbre.com", "zoho.com", "qnet.com", "scif.com", "mail.com", "mavtv.com", "frbsf.org", "epson.com", "bayer.com", 
					"tesla.com", "yahoo.com", "gmail.com", "intel.com", "sonic.net", "apple.com", "acphd.org", "waltdisney", "edf-re.com", "icumed.com", "davita.com", "abbott.com", "disney.com", "cydcor.com", "mindspring.com", "borregohealth.org", 
					"yandex.com", "google.com", "icloud.com", "tubitv.com", "nvidia.com", "vna.health", "legionm.com", "samsung.com", "calfund.org", "tinyurl.com", "phcdocs.org", "verizon.net", "anaheim.net", "oscars.org", "pmhd.org", 
					"verizon.com", "comcast.net", "hotmail.com", "outlook.com", "udacity.com", "chevron.com", "firstam.com", "agilent.com", "8minute.com", "altamed.org", "hotbench.tv", "pacbell.net", "netscape.net", "studio71us.com", 
					"rivcoda.org", "gvhomes.com", "deloitte.com", "facebook.com", "linkedin.com", "teamwass.com", "qualcomm.com", "bankofamerica", "viacomcbs.com", "paramount.com", "dollskill.com", "theagencyre.com", "unitedtalent.com", 
					"earthlink.net", "sbcglobal.net", "coveredca.com", "viacomcbs.com", "fremantle.com", "ovationtv.com", "peacocktv.com", "lucasfilm.com", "ingrooves.com", "livenation.com", "wmeagency.com", "psln.com", "cbbank.com", 
					"warnerbros.com", "wellsfargo.com", "roadrunner.com", "brightview.com", "rocketmail.com", "protonmail.com", "salesforce.com", "bridgebank.com", "montecito.bank", "silvergate.com", 
					"aidshealth.org", "uclahealth.org", "smartvoter.org", "servicenow.com", "progressive.com", "playstation.com", "virginorbit.com", "aegpresents.com", "checkerspot.com", "kernmedical.com", 
					"hingehealth.com", "denovahomes.com", "homebaseccc.org", "sonypictures.com", "eastwestbank.com", "carbonhealth.com", "nbcuniversal.com", "landseahomes.com", "scrippshealth.org", 
					"commercecasino.com", "universalmusic.com", "relativityspace.com", "adventisthealth.org", "molinahealthcare.com", "mrcentertainment.com", "createmusicgroup.com", "firstfoundationinc.com", 
					"stanfordhealthcare.org", "thecheesecakefactory.com", "entertainmentstudios.com", "discoverybehavioralhealth.com"]
					
		unwanted3 = [".edu", ".gov", ".mil", "ca.us", "1cbank", "qq.com", "me.com", "cs.com", "fox.com", "gmx.us", "bit.ly", "uhc.com", "wmg.com", "dga.org", "cnb.com", "msn.com", "att.net", "aol.com", "mac.com", "zorro.com", 
					"gmx.com", "hpe.com", "divx.com", "iehp.org", "ah4r.com", "khov.com", "sfhp.org", "cbre.com", "zoho.com", "qnet.com", "scif.com", "mail.com", "mavtv.com", "frbsf.org", "epson.com", "bayer.com", 
					"tesla.com", "yahoo.com", "gmail.com", "intel.com", "sonic.net", "apple.com", "acphd.org", "waltdisney", "edf-re.com", "icumed.com", "davita.com", "abbott.com", "disney.com", "cydcor.com", 
					"yandex.com", "google.com", "icloud.com", "tubitv.com", "nvidia.com", "vna.health", "legionm.com", "samsung.com", "calfund.org", "tinyurl.com", "phcdocs.org", "verizon.net", "anaheim.net", 
					"verizon.com", "comcast.net", "hotmail.com", "outlook.com", "udacity.com", "chevron.com", "firstam.com", "agilent.com", "8minute.com", "altamed.org", "hotbench.tv", "pacbell.net", "softbank.com", "dmg-entertainment.com", 
					"rivcoda.org", "gvhomes.com", "deloitte.com", "facebook.com", "linkedin.com", "teamwass.com", "qualcomm.com", "bankofamerica", "viacomcbs.com", "paramount.com", "dollskill.com", "fandango.com", "bronstudios.com", 
					"earthlink.net", "sbcglobal.net", "coveredca.com", "viacomcbs.com", "fremantle.com", "ovationtv.com", "peacocktv.com", "lucasfilm.com", "ingrooves.com", "livenation.com", 
					"warnerbros.com", "wellsfargo.com", "roadrunner.com", "brightview.com", "rocketmail.com", "protonmail.com", "salesforce.com", "bridgebank.com", "montecito.bank", "silvergate.com", 
					"aidshealth.org", "uclahealth.org", "smartvoter.org", "servicenow.com", "progressive.com", "playstation.com", "virginorbit.com", "aegpresents.com", "checkerspot.com", "kernmedical.com", 
					"hingehealth.com", "denovahomes.com", "homebaseccc.org", "sonypictures.com", "eastwestbank.com", "carbonhealth.com", "nbcuniversal.com", "landseahomes.com", "scrippshealth.org", 
					"commercecasino.com", "universalmusic.com", "relativityspace.com", "adventisthealth.org", "molinahealthcare.com", "mrcentertainment.com", "createmusicgroup.com", "firstfoundationinc.com", 
					"discoverylandco.com", "stanfordhealthcare.org", "thecheesecakefactory.com", "entertainmentstudios.com", "discoverybehavioralhealth.com", "paradigmagency.com", "unitedtalent.com"]

		unwanted4 = sorted(set(unwanted2 + unwanted3), key=len)

		print(unwanted4)

	def random_processing2(self):
		df = pd.read_csv("ca-lawyers-with-email-and-website-da-score-30-batch-2.csv", na_filter=False)

		print(df[df["Law Firm"].str.contains("food", case=False, na=False)]["Website"].unique().tolist())
		print(df[df["Website"].str.contains("food", case=False, na=False)]["Website"].unique().tolist())
		print(df[df["Email"].str.contains("food", case=False, na=False)]["Website"].unique().tolist())

	def random_processing3(self):
		df = pd.read_csv("ca-lawyers-with-email-and-website-da-score-30-final-list.csv", na_filter=False)

		print(len(df["Website"].unique()))

		df1 = df[df["Email"] == "not available"]
		df1 = df1.reset_index(drop=True)

		print(df1.head())
		print(df1.tail())
		print(len(df1))

		df2 = df[df["Law Firm"] == ""]
		df2 = df2.reset_index(drop=True)

		print(df2.head())
		print(df2.tail())
		print(len(df2))

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
	#scraper.final_processing8()
	#scraper.final_processing9()
	#scraper.final_processing11()
	#scraper.final_processing12()
	#scraper.final_processing13()
	#scraper.final_processing14()
	scraper.random_processing3()

	end = time.time()

	execution_time = end - start

	print("Execution time: {} seconds".format(execution_time))

if __name__ == "__main__":
	main()



