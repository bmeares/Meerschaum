### NOTE: This adds the parent Meerschaum directory to PATH to import the development version
###       instead of the system version.
###
### If you are running this on Windows or don't have the development version cloned,
### simply comment out the below line.
import sys; sys.path.insert(0, '../../')

### SSL fix
import requests, urllib3
requests.packages.urllib3.disable_warnings()
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':HIGH:!DH:!aNULL'
try:
  requests.packages.urllib3.contrib.pyopenssl.util.ssl_.DEFAULT_CIPHERS += ':HIGH:!DH:!aNULL'
except:
  pass

### webdriver with features from the normal requests lib
from seleniumrequests import Firefox
### we need options to start a headless firefox instance
from selenium.webdriver.firefox.options import Options
### the below imports are needed to wait for elements to load
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

xpaths = {
    'initial_login' : "/html/body/div/div/div/main/div/div/div[2]/div/form/button",
    'username' : "/html/body/div/div/div/main/div/div/div[4]/div[1]/form/div[1]/input",
    'password' : "/html/body/div/div/div/main/div/div/div[4]/div[1]/form/div[2]/input",
    'login' : "/html/body/div/div/div/main/div/div/div[4]/div[1]/form/button",
}
urls = {
    'login' : "https://public-apps.apexclearing.com/session/#/login/",
    'activities' : "https://public-api.apexclearing.com/activities-provider/api/v1/activities/",
}

def ask_for_credentials():
    """
    Prompt the user for login information and update the Meerschaum configuration file.
    """
    from getpass import getpass
    username = input("Apex Username: ")
    password = getpass(prompt="Apex Password: ")
    account  = input("Apex account number: ")
    if 'plugins' not in cf: cf['plugins'] = {}
    if 'apex' not in cf['plugins']: cf['plugins']['apex'] = {}
    if 'login' not in cf['plugins']['apex']: cf['plugins']['apex']['login'] = {}
    cf['plugins']['apex']['login']['username'] = username
    cf['plugins']['apex']['login']['password'] = password
    cf['plugins']['apex']['login']['account'] = account
    write_config(cf)
    return username, password, account

### modules we'll need later
import meerschaum as mrsm
from meerschaum.utils.debug import dprint
from meerschaum.utils.warnings import warn, error
from meerschaum.utils.misc import import_pandas
pd = import_pandas()
import datetime

### get credentials from Meerschaum config or the user
from meerschaum.config import config as cf, write_config
while True:
    try:
        apex_username = cf['plugins']['apex']['login']['username']
        apex_password = cf['plugins']['apex']['login']['password']
        apex_account  = cf['plugins']['apex']['login']['account']
    except:
        ask_for_credentials()
    else:
        break

### init the driver in main
driver = None

def apex_login(debug : bool = False):
    if debug: dprint("Loading login page...")
    driver.get(urls['login'])

    ### enter username on first page
    if debug: dprint("Waiting for first username textbox...")
    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.NAME, 'username')))
    initial_login = driver.find_element_by_name("username")
    initial_login.clear()
    initial_login.send_keys(apex_username)
    initial_login.find_element_by_xpath(xpaths['initial_login']).click()

    #### enter username on real login form
    if debug: dprint("Waiting for second username textbox...")
    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, xpaths['username'])))
    user_login = driver.find_element_by_xpath(xpaths['username'])
    user_login.clear()
    user_login.send_keys(apex_username)

    ### enter password
    if debug: dprint("Waiting for password textbox...")
    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, xpaths['password'])))
    user_pass = driver.find_element_by_xpath(xpaths['password'])
    user_pass.clear()
    user_pass.send_keys(apex_password)

    ### click login
    if debug: dprint("Clicking login button...")
    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, xpaths['login'])))
    driver.find_element_by_xpath(xpaths['login']).click()

    ### enter account number and press Enter
    if debug: dprint("Waiting for account textbox...")
    try:
        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.ID, 'account')))
    except TimeoutException:
        driver.quit()
        error('Incorrect login. Please check the login information with `mrsm edit config` under plugins:apex:login')
    account_login = driver.find_element_by_id('account')
    account_login.clear()
    account_login.send_keys(apex_account)
    account_login.send_keys(u'\ue007')

def get_activities(
        activity_types : list = ['TRADES', 'MONEY_MOVEMENTS', 'POSITION_ADJUSTMENTS'],
        start_date : datetime.date = None,
        end_date : datetime.date = datetime.date.today(),
        debug : bool = False
    ) -> pd.DataFrame:
    """
    Get activities data from Apex and return a pandas dataframe
    """
    dfs = []
    if start_date is None: start_date = end_date.replace(year=end_date.year - 2)
    for activity_type in activity_types:
        url = (
            urls['activities'] +
            apex_account +
            f"?activityType={activity_type}" +
            f"&startDate={start_date}" +
            f"&endDate={end_date}"
        )
        if debug: dprint(f"Fetching data from URL: {url}")
        response = driver.request('GET', url)
        df = pd.read_json(response.text)
        ### parse the list column as a string
        try:
            df['descriptionLines'] = df['descriptionLines'].apply(lambda x : "\n".join(x))
        except KeyError:
            pass
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True).sort_values(by='timestamp').reset_index(drop=True)

def main():
    global driver
    browser_options = Options()
    browser_options.add_argument('--headless')
    browser_options.add_argument('--window-size=1920x1080')
    driver = Firefox(options=browser_options)

    ### create and configure the Pipes
    activities_pipe = mrsm.Pipe('apex', 'activities')
    activities_pipe.columns = {'datetime' : 'timestamp'}
    running_dividends_pipe = mrsm.Pipe('sql:main', 'apex_running_dividends')
    running_dividends_pipe.parameters = {
        'columns' : {
            'datetime' : 'timestamp',
        },
        'fetch' : {
            'definition' : """
            SELECT DISTINCT timestamp, sum("netAmount") OVER (ORDER BY timestamp ASC) AS "running_dividends"
            FROM apex_activities
            WHERE symbol IS NOT NULL AND symbol != ''
            AND "transferDirection" = 'INCOMING'
            AND "activityType" = 'MONEY_MOVEMENTS'
            """,
        },
    }

    ### log in to set the session cookies
    apex_login(debug=True)
    start_date = activities_pipe.sync_time
    if start_date: start_date = start_date.date().replace(day=start_date.day - 1)
    df = get_activities(start_date=start_date, debug=True)

    activities_pipe.sync(df, debug=True)
    running_dividends_pipe.sync(debug=True)

    driver.quit()

if __name__ == "__main__":
    main()
