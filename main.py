import csv
import os
import re
import json
import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone

from playwright.async_api import async_playwright, expect
import requests
from otp_email_fetcher import get_panel_otp

from logger_config import setup_logger, TRACE_LEVEL
from const import *
logger = setup_logger(log_level=TRACE_LEVEL)

class ReportScraper:

    admin_email: str = None
    admin_password: str = None

    def __init__(self, file_path: str, output_folder: str = "output", max_leads_per_request: int = 500, headless: bool = True):
        """
        Initialize the ReportScraper with the given file path and output folder.
        Additionally, set the maximum number of leads to fetch per request and the headless mode.
        We also initialize the clients DataFrame and the all_rows DataFrame to store the fetched leads.

        """

        logger.info("Initializing ReportScraper...")
        
        self.clients = pd.read_excel(file_path)
        self.all_rows = pd.DataFrame()
        self.panel_url = None
        self.masterdata_url = None
        self.masterdata_all = dict()
        self.user_email = None
        self.user_password = None   
        self.browser = None
        self.context = None
        self.page = None
        self.headless = headless
        self.max_leads_per_request = max_leads_per_request
        self.is_scraping_complete = asyncio.Event()
        
        # Create the output folder if it doesn't exist
        self.output_folder = output_folder
        os.makedirs(self.output_folder, exist_ok=True)

        self.output_folder_results = "results"
        os.makedirs(self.output_folder_results, exist_ok=True)

        logger.info(f"Output folder created at: {self.output_folder}")

    async def wait_for_selectors(self, page, selectors, timeout=30000):
        """
        Wait for the given selectors to appear on the page.
        """
        logger.info("Waiting for selectors...")
        tasks = {key: asyncio.create_task(page.wait_for_selector(selector, timeout=timeout)) for key, selector in selectors.items()}
        done, pending = await asyncio.wait(tasks.values(), return_when=asyncio.FIRST_COMPLETED)
        
        for pending_task in pending:
            pending_task.cancel()

        for key in selectors.keys():
            if key in done:
                logger.info(f"Selector '{key}' found.")
            else:
                logger.warning(f"Selector '{key}' not found within timeout.")
        return done, tasks

    async def handle_otp_input(self, page, max_retries=10):
        """
        Handle the OTP input modal.
        """
        logger.info("Handling OTP input...")

        for attempt in range(max_retries):
            logger.info(f"Attempt {attempt + 1}/{max_retries}: Clicking submit modal...")
            await page.click("#submitModal")
            await page.wait_for_selector(".otp-input-frame")

            if not (panel_otp := await asyncio.to_thread(
                get_panel_otp, ReportScraper.admin_email,
                ReportScraper.admin_password,
                current_time=datetime.now(timezone.utc) - timedelta(seconds=1)
            )): 
                logger.error("Failed to fetch OTP.")
                await page.click("#cancelModal")
                return

            otp_inputs = await page.query_selector_all(".otp-input")
            for index, digit in enumerate(panel_otp):
                await otp_inputs[index].fill(digit)

            await page.click("#btnSave")

            selectors_after_otp = {
                "error": "div.error-msg",
                "dashboard": "div.title:has-text('Dashboard Index')",
                "alert": "span.login-alert-lable:has-text('Login Alert!')"
            }

            done, tasks = await self.wait_for_selectors(page, selectors_after_otp)
            for task in done:
                if task == tasks["error"]:
                    logger.error("Error: The OTP you entered is incorrect.")
                    logger.info(f"Retrying... ({attempt + 1}/{max_retries})")
                    await page.click("#cancelModal")
                    break

                elif task == tasks["alert"]:
                    logger.info("Proceeding to the dashboard...")
                    await page.click("#submitModal")  # Keeping this line unchanged
                    return True

                elif task == tasks["dashboard"]:
                    logger.info("Proceeding to the dashboard...")
                    return True
            
            await page.wait_for_timeout(1000)

        logger.error("Max retries reached. Exiting.")
        return False
    
    async def handle_request(self, request):
        """
        Handle the request to capture the master data URL.
        """
        if "api/MasterData/all" in request.url:
            self.masterdata_url = request.url
            logger.info(self.masterdata_url)
            self.page.remove_listener("request", self.handle_request)
            self.is_scraping_complete.set()

    async def handle_response(self, response):
        """
        Handle the response to capture the lead data.
        """
        try:
            logger.debug(f"Received response: {response.url} with status {response.status}")

            if (response.status == 200 and
                response.headers.get("content-type", "").startswith("application/json") and
                "api/reports/drillDownData" in response.url
            ):
                body = await response.body()
                json_data = await asyncio.to_thread(json.loads, body.decode("utf-8"))

                report_data = json_data.get("data", {}).get("reportData", [])
                
                if report_data and self.all_rows.shape[0] != self.total_lead:
                    new_rows = pd.DataFrame(report_data)
                    
                    # Concatenate new rows and drop duplicates if any exist
                    self.all_rows = pd.concat([self.all_rows, new_rows], ignore_index=True).drop_duplicates()

                    logger.info(f"Total unique leads fetched: {self.all_rows.shape[0]}/{self.total_lead}")
                    logger.trace(f"Fetched rows: {new_rows}")  # Log newly fetched rows
                else:
                    logger.info("No more leads to fetch or already fetched all leads. Exiting...")
            
            if response.status == 200 and "api/MasterData/all" in response.url:
                self.masterdata_url = response.url
                print(self.masterdata_url)

            if self.total_lead == self.all_rows.shape[0]:
                logger.info("Total leads count reached. Removing response listener.")
                self.page.remove_listener("response", self.handle_response)
                await self.save_leads(self.domain().lower() + "_leads.csv")

                self.is_scraping_complete.set()
                logger.info("Scraping complete. Leads saved.")
                return
        
        except Exception as e:
            logger.error(f"Error retrieving response body: {e}")
            self.is_scraping_complete.set()


    def domain(self) -> str:
        """
        Extract the domain from the panel URL.
        """
        pattern = r"(?:https?://)?(?:www\.)?([^:/\n?]+)"
        match = re.match(pattern, self.panel_url)
        
        if match:
            logger.debug(f"Extracted domain: {match.group(1)} from panel URL: {self.panel_url}")
            return match.group(1)
        
        logger.warning("No domain found in panel URL.")
        return None

    def map_masterdata(self):
        """
        Fetch and group the master data from the captured URL.
        """
        data = requests.get(self.masterdata_url).json()
        grouped_data = {}
        for key, value in data.items():
            str_key = str(key)
            if isinstance(value, list):
                grouped_data[str_key] = {}
                for item in value:
                    if isinstance(item, dict):
                        grouped_data[str_key][str(item.get("id"))] = str(item.get("name"))
                    else:
                        grouped_data[str_key][str(item)] = None  # Convert item to string
            else:
                grouped_data[str_key] = str(value)
        return grouped_data

    async def save_leads(self, file_name):
        """
        Save the leads to a CSV file.
        """
        masterdata = self.map_masterdata()

        if not os.path.exists("master"):
            os.makedirs("master")
        with open(f"master/masterdata_{self.domain().lower()}.json", "w") as f:
            json.dump(masterdata, f, indent=4)

        # If the total leads per request is equal to the total leads, wait for 2 seconds
        # to ensure headers are loaded before extracting them

        if self.max_leads_per_request <= self.total_lead:
            await self.page.wait_for_timeout(2000)

        data = {}
        headers = await self.page.query_selector_all('.ag-header-cell')
        for header in headers:
            col_id = await header.get_attribute('col-id')
            header_text = await header.query_selector('span[ref="eText"]')
            if header_text:
                header_text = await header_text.inner_text()

            if col_id and header_text:
                data[col_id] = header_text
        logger.debug(f"Data: {data}")
        
        # We need to map the fields to the corresponding master data
        # Currently, we are only mapping the fields that are present in the const.py file
        #
        # E.g., _map_fields = data or fields_dict
        # Since, data dict key are not correctly mapped with the master data to dynamically,
        # handle the mapping, we will use the fields_dict to map the fields.

        _map_fields = fields_dict
        for key in self.all_rows.columns:
            self.all_rows[key] = self.all_rows[key].astype(str)
        
        for index, row in self.all_rows.iterrows():
            for key, value in row.items():
                if apiKey := _map_fields.get(key):
                    master_value = masterdata.get(apiKey)
                    if master_value is not None:
                        ids = [id.strip() for id in str(value).split(',') if id.strip()]
                        corresponding_values = []
                        for id in ids:
                            master_val = master_value.get(id)
                            if master_val is not None:
                                corresponding_values.append(master_val)
                        result = ', '.join(corresponding_values)
                        if result:
                            self.all_rows.at[index, key] = result

        for column in self.all_rows.columns:
            if column in masterdata:
                self.all_rows[column] = self.all_rows[column].map(masterdata[column])

        full_file_path = os.path.join(self.output_folder, file_name)
        logger.info(f"Saving leads to {full_file_path}...")

        try:
            self.all_rows = self.all_rows.apply(
                lambda x: x.map(
                    lambda y: str(y).replace('\r\n', '\\n').replace('\r', '\\n').replace('\n', '\\n') if isinstance(y, str) else y
                )
            )
            self.prev_headers = self.all_rows.columns.copy()

            self.all_rows.columns = data.values()
            self.all_rows.to_csv(full_file_path, index=False, quoting=csv.QUOTE_ALL, encoding='utf-8')
            logger.info("Leads successfully saved as CSV to output folder.")

            ############################################################################################################
            # The following code is for saving the leads to results folder after modification of the fields and values #
            ############################################################################################################

            self.all_rows.columns = self.prev_headers
            
            cols_to_select = ['super_camp_id', 'Mobile', 'Email', 'feedback_id', 'comment']

            self.all_rows['feedback_id'] = ''
            self.all_rows['comment'] = ''
            self.all_rows['super_camp_id'] = ''
            self.all_rows.rename(columns={'MobileNumber': 'Mobile'}, inplace=True)

            # Only selecting the cols_to_select columns
            self.all_rows = self.all_rows[cols_to_select]
            self.all_rows.to_csv(os.path.join(self.output_folder_results, file_name), index=False, quoting=csv.QUOTE_ALL, encoding='utf-8')
            logger.info("Modified leads successfully saved as CSV to results folder.")

        except Exception as e:
            logger.error(f"Error saving leads: {e}")

    def custom_options_script(self, total_leads_per_page):
        return f"""
        var selectElement = document.getElementById('custom-pagination-select');
        var newOption = document.createElement('option');
        newOption.text = '{total_leads_per_page}';
        newOption.value = '{total_leads_per_page}';
        newOption.classList.add('dropdown-options');
        selectElement.appendChild(newOption);
    """


    def auto_next_page_script(self):
        return """
        const intervalId = setInterval(() => {
            const targetElement = document.querySelector("#DrillDownTable > div:nth-child(3) > div > div > div > div.ag-root-wrapper-body.ag-layout-normal.ag-focus-managed > div.ag-root.ag-unselectable.ag-layout-normal > div.ag-overlay.ag-hidden > div > div");
            const clickElement = document.querySelector("#drill-down-table-pagination > div.right-arrow.icon");

            if (targetElement && targetElement.innerHTML.trim() === "" && clickElement) {
                clickElement.click();
            } else if (!targetElement) {
                console.log("Target element not found");
            } else {
                console.log("Target element is not empty");
            }
        }, 2000);
        """

    async def process_page(self, client_data):
 
        # Extract the panel link
        self.panel_url = client_data['Panel Link']
        if not self.panel_url.startswith('https://'):
            self.panel_url = 'https://' + self.panel_url

        # Extract the client name
        self.client_name = client_data['Client Name']
        logger.info(f"Processing client: {self.client_name} with panel URL: {self.panel_url}")

        try:
            self.page.on("request", self.handle_request)
            await self.page.goto(self.panel_url)
            await self.is_scraping_complete.wait()
            self.is_scraping_complete.clear()

        except TimeoutError:
            logger.error("The operation timed out. Refreshing the page.")
            try:
                await self.page.reload(wait_until='domcontentloaded')
            except TimeoutError:
                logger.error("The operation timed out again. Exiting.")
                self.is_scraping_complete.set()
                return

        await expect(self.page).to_have_title(re.compile(r".*Lead\sManagement\sPlatform.*"))

        panel_title = await self.page.title()
        logger.debug(f"Panel title: {panel_title}")

        await self.page.fill("#userName", client_data["Panel Email"])
        await self.page.fill("#password", client_data["Panel Password"])

        await self.page.click("#btnLogin")

        selectors_after_login = {
            "error": "label.error",
            "otp": ".modal-title:has-text('Send OTP')"
        }

        try:
            done, tasks = await self.wait_for_selectors(self.page, selectors_after_login)
            for task in done:
                if task == tasks["error"]:
                    logger.error("Error: The provided credentials are incorrect.")
                    self.is_scraping_complete.set()
                    return

                elif task == tasks["otp"]:
                    logger.info("Success: OTP modal is displayed, proceeding...")
                    if not await self.handle_otp_input(self.page):
                        self.is_scraping_complete.set()
                        return

        except asyncio.TimeoutError:
            logger.warning("Neither the error message nor the OTP modal appeared in the timeout.")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")

        try:
            await self.page.click("div.description:has-text('Leads and Applications')")

            logger.info("Waiting for the page to load...")

            try:
                total_lead_text = await self.page.text_content("#ReportNumberPage > .numberCard-container > div:nth-child(2) .card-count")
                self.total_lead = int(total_lead_text.strip())
                logger.info(f"Total leads: {self.total_lead}")
            except Exception as e:
                logger.error(f"Error retrieving total leads: {e}")
                self.total_lead = 0  # Set a default value in case of error

            if self.total_lead == 0:
                logger.info("No leads found for this client.")
                self.is_scraping_complete.set()
                return

            await self.page.click('#ReportNumberPage > .numberCard-container > div:nth-child(2) .card-right-arrow')

            # Report page ~
            self.page.on("response", self.handle_response)

            await self.page.wait_for_load_state("domcontentloaded") # Wait for dom state to be loaded
            await self.page.wait_for_selector('div[role="row"][row-index="0"][aria-rowindex="2"][aria-label="Press SPACE to select this row."]',  state='attached')

            total_leads_per_page = str(min(self.total_lead, self.max_leads_per_request))
            logger.info(f"Fetching {total_leads_per_page} leads per request.")


            await self.page.wait_for_selector('[role="rowgroup"] > div:nth-of-type(2)')
            await self.page.wait_for_selector('[ref="eHeaderContainer"] span[ref="eText"]')

            await self.page.evaluate(
                """document.querySelector('#DrillDownTable > div:last-child > div').style.width = '999999px';"""
            )
            await self.page.evaluate(self.custom_options_script(total_leads_per_page))

            await self.page.select_option("select#custom-pagination-select", value=total_leads_per_page)
            await self.page.evaluate(self.auto_next_page_script())

        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
        finally:
            pass


    async def run(self):
        async with async_playwright() as playwright:
            self.browser = await playwright.chromium.launch(headless=self.headless, args=[""])
            logger.info("Browser launched successfully.")

            for index, client_data in self.clients.iterrows():
                if index in []: # Add the indices of clients to skip
                    logger.warning("Skipping client: " + client_data["Client Name"] + "...")
                    continue

                self.context = await self.browser.new_context()
                self.context.set_default_timeout(60000)

                self.page = await self.context.new_page()

                await self.process_page(client_data)
                await self.is_scraping_complete.wait()
                logger.info(f"Completed processing for client: {client_data['Client Name']}.")

                await self.context.close()
                self.is_scraping_complete.clear()
                self.all_rows = pd.DataFrame()
                
                await asyncio.sleep(5)


if __name__ == "__main__":
    ReportScraper.admin_email = "ops.reports@dummy.com"
    ReportScraper.admin_password = "Cnpj$k)sW43n2Cf!"
    asyncio.run(
        ReportScraper(
            file_path="Extraaedge Clients.xlsx", 
            output_folder="reports", 
            max_leads_per_request=1000,
            headless=False
        ).run()
    )

# Existing Issues:
# The script is unable to correctly inject the custom options—this may be due to the page not being fully loaded. (fixed)
# The script sometimes shows an OTP error even when the OTP is correct and the page has loaded. (semi-fixed)
# Sometimes the report page stays blank—consider implementing a script to auto-refresh the page 
# and re-inject all JavaScript if the content does not load. (semi-fixed)

