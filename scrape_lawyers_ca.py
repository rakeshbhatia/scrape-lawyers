import re
import os
import time
import json
import numpy
import string
import random
import asyncio
import requests
import zipcodes
import itertools
import pandas as pd
import multiprocessing
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
		#options.add_argument('--headless')
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

	def clean_lawyers_data(self):
		df = pd.read_csv("lawyers-ca-data.csv")

		# Extract all lawyers with website
		df1 = df.loc[df["Website"] != "Not Available"]
		df1["Address"] = df1["Address"].str.lower()
		df1["Address"] = df1["Address"].str.upper()
		#df1["Email"] = df1["Email"].str.lower()
		df1["Website"] = df1["Website"].str.lower()

		print(df1.head())
		print(df1.tail())
		print(len(df1))

		# Sort by city
		df1 = df1.sort_values(by=["City"], ignore_index=True)

		df1.to_csv("ca-lawyers-with-website-1.csv")

		# Extract all lawyers without website and with email
		df2 = df.loc[(df2["Email"] != "Not Available") & (df["Website"] == "Not Available")]

		df2["Address"] = df2["Address"].str.lower()
		df2["Address"] = df2["Address"].str.upper()
		df2["Email"] = df2["Email"].str.lower()
		#df2["Website"] = df2["Website"].str.lower()

		# Remove lawyers with unwanted email providers
		df2 = df2[~df2["Email"].str.contains("checkerspot.com")]
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
		df2 = df2[~df2["Email"].str.contains(".gov")]

		

		df2 = df2.sort_values(by=["City"], ignore_index=True)

		print(df2.head())
		print(df2.tail())
		print(len(df2))


	def scrape_da_score(self):
		print("Scraping DA score")

		# Get new driver
		driver = self.browser()
		driver.get(self.da_url)

		text = "https://savinbursklaw.com/"

		search = driver.find_element(By.ID, "url")
		search.send_keys(text)

		button = driver.find_element(By.ID, "sub_url")
		button.click()

		time.sleep(1)

		#soup = self.get_soup(driver.page_source)
		#da_score = soup.find_all("td")[3].text

		xpath = "/html/body/div[3]/div/div/div/div/div[2]/table/tbody/tr[2]/td[2]"
		da_score = driver.find_element(By.XPATH, xpath).text
		print("Domain Authority: ", da_score)

		driver.quit()

	def scrape_all_da_scores(self):
		print("Scraping all DA scores")
		self.scrape_da_score()

def main():
	start = time.time()

	dem_url = "https://apps.calbar.ca.gov/members/demographics_search.aspx"
	da_url = "https://www.dachecker.org"

	scraper = AttorneyScraper(dem_url, da_url)
	#scraper.get_zip_codes()
	#scraper.clean_zip_codes()
	#scraper.search_lawyer_basic("94536")
	#scraper.search_all_lawyers_basic()
	#scraper.search_all_lawyers_advanced()
	#scraper.scrape_all_lawyers()
	scraper.clean_lawyers_data()

	end = time.time()

	execution_time = end - start

	print("Execution time: {} seconds".format(execution_time))

if __name__ == "__main__":
	main()



