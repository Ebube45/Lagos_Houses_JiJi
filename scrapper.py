import httpx
import pandas as pd
from bs4 import BeautifulSoup
from retrying import retry
from playwright.sync_api import sync_playwright

# Define a retry decorator with exponential backoff
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=3)
def scrape_house_listings(url):
    base_url = "https://jiji.ng"

    # Function to scrape additional details including attributes and description
    def scrape_additional_details(link):
        try:
            response = httpx.get(link, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract attribute values
            attribute_tags = soup.select(".b-advert-attributes-wrapper.b-advert-icon-attributes .b-advert-icon-attribute span")
            attributes = [tag.text.strip() for tag in attribute_tags]

            # Extract additional details
            additional_details_tags = soup.select(".b-advert-attribute")
            additional_details = {}
            for tag in additional_details_tags:
                key = tag.find(class_="b-advert-attribute__key").text.strip()
                value = tag.find(class_="b-advert-attribute__value").text.strip()
                # Exclude Bedrooms and Toilets
                if key.lower() not in ['bedrooms', 'toilets']:
                    additional_details[key] = value

            # Extract description
            description = soup.select_one(".qa-advert-description.b-advert__description-text")
            description_text = description.text.strip() if description else None

            # Combine all extracted details
            extracted_details = {
                "Description": description_text,
                **additional_details,
                **{"Attribute_" + str(i+1): attr for i, attr in enumerate(attributes)}
            }

            return extracted_details
        except Exception as e:
            print(f"Error occurred while scraping additional details for link: {link}")
            print(e)
            return {}

    # Set up Playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        # Load the Jiji.ng page
        page.goto(url)
        page.wait_for_timeout(2000)  # Wait for the page to load

        # Scroll down the page to load more content only 5 times
        for _ in range(120):
            page.evaluate('window.scrollTo(0, document.body.scrollHeight);')
            page.wait_for_timeout(5000)  # Wait for the page to load

        # Fetch the house listing page content
        house_html_content = page.content()

        # Close the browser
        browser.close()

    # Parse the HTML content using BeautifulSoup
    house_soup = BeautifulSoup(house_html_content, 'html.parser')

    # Find all the house listing elements
    house_listings = house_soup.find_all(class_="b-list-advert__gallery__item js-advert-list-item")

    listings = []
    # Iterate over each listing to extract details
    for listing in house_listings:
        price_div = listing.select_one(".qa-advert-price")
        price = price_div.text.strip().replace('₦', '').replace(',', '')

        name_div = listing.select_one(".b-advert-title-inner.qa-advert-title.b-advert-title-inner--div")
        name = name_div.text.strip() if name_div else ""

        location_div = listing.select_one(".b-list-advert__region__text")
        location = location_div.text.strip()

        link_div = listing.select_one(".b-list-advert-base.qa-advert-list-item.b-list-advert-base--gallery")
        if link_div:
            link = link_div.get("href", "")
            full_link = base_url + link
            # If name is missing, extract it from the link
            if not name:
                name = link.split('/')[-1].replace('-', ' ').title()
        else:
            full_link = ""

        # Extract additional details from the individual link
        additional_details = scrape_additional_details(full_link)

        listings.append({
            "name": name,
            "price": price,
            "location": location,
            "link": full_link,
            **additional_details  # Add additional details as separate columns
        })

    return listings

try:
    # Scrape house listings from the URL
    url = "https://jiji.ng/lagos/houses-apartments-for-rent"
    all_listings = scrape_house_listings(url)

    # Create DataFrame
    df = pd.DataFrame(all_listings)

    # Save DataFrame to CSV
    df.to_csv("house_listings.csv", index=False)

    print("House listings extracted and saved to house_listings.csv")
except Exception as e:
    print("An error occurred during scraping:")
    print(e)
