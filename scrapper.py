import pandas as pd
import httpx
from retrying import retry
from playwright.sync_api import sync_playwright
from selectolax.parser import HTMLParser

# Define a retry decorator with exponential backoff
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=3)
def scrape_house_listings(url):
    base_url = "https://jiji.ng"

    # Function to scrape additional details including attributes and description
    def scrape_additional_details(link):
        try:
            response = httpx.get(link, timeout=30)
            response.raise_for_status()  # Raise exception for non-200 status codes
            html_content = response.content
            tree = HTMLParser(html_content)
            
            # Extract attribute values
            attribute_tags = tree.css(".b-advert-attributes-wrapper.b-advert-icon-attributes .b-advert-icon-attribute span")
            attributes = [tag.text(strip=True) for tag in attribute_tags]

            # Extract additional details
            additional_details_tags = tree.css(".b-advert-attribute")
            additional_details = {}
            for tag in additional_details_tags:
                key = tag.css_first(".b-advert-attribute__key").text(strip=True)
                value = tag.css_first(".b-advert-attribute__value").text(strip=True)
                # Exclude Bedrooms and Toilets
                if key.lower() not in ['bedrooms', 'toilets']:
                    additional_details[key] = value

            # Extract description
            description = tree.css_first(".qa-advert-description.b-advert__description-text")
            description_text = description.text(strip=True) if description else None

            # Combine all extracted details
            extracted_details = {
                "Description": description_text,
                **additional_details,
                **{"Attribute_" + str(i+1): attr for i, attr in enumerate(attributes)}
            }

            return extracted_details
        except (httpx.ReadTimeout, httpx.HTTPStatusError) as e:
            print(f"An error occurred while scraping additional details for link: {link}. Error: {e}")
            return {}

    try:
        # Set up Playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            # Load the Jiji.ng page
            page.goto(url)
            page.wait_for_timeout(2000)  # Wait for the page to load

            # Scroll down the page to load more content only 5 times
            for _ in range(60):
                page.evaluate('window.scrollTo(0, document.body.scrollHeight);')
                page.wait_for_timeout(5000)  # Wait for the page to load

            # Fetch the house listing page content
            house_html_content = page.content()

            # Close the browser
            browser.close()
    except Exception as e:
        print(f"An error occurred while fetching the house listing page content. Error: {e}")
        return []

    try:
        # Parse the HTML content using Selectolax
        tree = HTMLParser(house_html_content)

        # Find all the house listing elements
        house_listings = tree.css(".b-list-advert__gallery__item.js-advert-list-item")

        listings = []
        # Iterate over each listing to extract details
        for listing in house_listings:
            price_div = listing.css_first(".qa-advert-price")
            price = price_div.text(strip=True).replace('₦', '').replace(',', '')

            name_div = listing.css_first(".b-advert-title-inner.qa-advert-title.b-advert-title-inner--div")
            name = name_div.text(strip=True) if name_div else ""

            location_div = listing.css_first(".b-list-advert__region__text")
            location = location_div.text(strip=True)

            link_div = listing.css_first(".b-list-advert-base.qa-advert-list-item.b-list-advert-base--gallery")
            if link_div:
                link = link_div.attributes.get("href", "")
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
    except Exception as e:
        print(f"An error occurred while parsing the HTML content. Error: {e}")
        return []

# Scrape house listings from the URL
url = "https://jiji.ng/lagos/houses-apartments-for-rent"
all_listings = scrape_house_listings(url)

if all_listings:
    # Create DataFrame
    df = pd.DataFrame(all_listings)

    # Save DataFrame to CSV
    df.to_csv("house_listings.csv", index=False)

    print("House listings extracted and saved to house_listings.csv")
else:
    print("No house listings were extracted.")
