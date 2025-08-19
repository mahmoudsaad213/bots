import re
import requests
import random
import json
import string
import time
import logging
import traceback
from datetime import datetime, timedelta
import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import psycopg2.pool # Import the pool module

# Telegram Bot API imports
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackContext
from telegram.helpers import escape_markdown

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Configuration from Environment Variables ---
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
DATABASE_URL = os.environ.get('DATABASE_URL')

if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable not set!")
    exit(1)
if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable not set!")
    exit(1)

# --- Database Connection Pool ---
conn_pool = None

def get_db_connection():
    """Establishes and returns a database connection from the pool."""
    global conn_pool
    if conn_pool is None:
        # Min 1, Max 10 connections. Adjust as needed.
        # dsn (Data Source Name) is the DATABASE_URL
        conn_pool = psycopg2.pool.SimpleConnectionPool(1, 10, dsn=DATABASE_URL) 
    return conn_pool.getconn()

def release_db_connection(conn):
    """Releases a database connection back to the pool."""
    global conn_pool
    if conn_pool:
        conn_pool.putconn(conn)

def init_db():
    """Initializes the database schema if it doesn't exist."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users_tempmail_config (
                user_id BIGINT PRIMARY KEY,
                tempmail_api_key TEXT NOT NULL,
                current_email TEXT,
                email_created_at DATE,
                processed_uuids TEXT[] DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            -- Add a trigger to update 'updated_at' automatically
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            DROP TRIGGER IF EXISTS update_users_tempmail_config_updated_at ON users_tempmail_config;
            CREATE TRIGGER update_users_tempmail_config_updated_at
            BEFORE UPDATE ON users_tempmail_config
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        """)
        conn.commit()
        logger.info("Database schema initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_db_connection(conn)

# --- Database Operations for User TempMail Config ---

def get_user_tempmail_config(user_id: int) -> dict | None:
    """Retrieves TempMail configuration for a given user_id from the database."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT tempmail_api_key, current_email, email_created_at, processed_uuids FROM users_tempmail_config WHERE user_id = %s",
            (user_id,)
        )
        row = cur.fetchone()
        if row:
            return {
                "api_key": row[0],
                "current_email": row[1],
                "email_created_date": row[2], # This will be a datetime.date object
                "processed_uuids": set(row[3]) if row[3] else set() # Convert list to set for faster lookup
            }
        return None
    except Exception as e:
        logger.error(f"Error getting TempMail config for user {user_id}: {e}")
        return None
    finally:
        if conn:
            release_db_connection(conn)

def save_user_tempmail_config(user_id: int, api_key: str, current_email: str = None, email_created_date: datetime.date = None, processed_uuids: set = None):
    """Saves or updates TempMail configuration for a user in the database."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Convert set to list for PostgreSQL array
        uuids_list = list(processed_uuids) if processed_uuids else []

        cur.execute(
            """
            INSERT INTO users_tempmail_config (user_id, tempmail_api_key, current_email, email_created_at, processed_uuids)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                tempmail_api_key = EXCLUDED.tempmail_api_key,
                current_email = EXCLUDED.current_email,
                email_created_at = EXCLUDED.email_created_at,
                processed_uuids = EXCLUDED.processed_uuids,
                updated_at = CURRENT_TIMESTAMP;
            """,
            (user_id, api_key, current_email, email_created_date, uuids_list)
        )
        conn.commit()
        logger.info(f"TempMail config saved/updated for user {user_id}.")
    except Exception as e:
        logger.error(f"Error saving TempMail config for user {user_id}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_db_connection(conn)

def update_user_email_and_uuids(user_id: int, email: str, email_date: datetime.date, uuids: set):
    """Updates current_email, email_created_at, and processed_uuids for a user."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        uuids_list = list(uuids) if uuids else []
        cur.execute(
            """
            UPDATE users_tempmail_config
            SET current_email = %s,
                email_created_at = %s,
                processed_uuids = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = %s;
            """,
            (email, email_date, uuids_list, user_id)
        )
        conn.commit()
        logger.info(f"User {user_id} email and UUIDs updated.")
    except Exception as e:
        logger.error(f"Error updating email/uuids for user {user_id}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_db_connection(conn)

def update_user_processed_uuids(user_id: int, new_uuids: set):
    """Updates only the processed_uuids for a user."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        uuids_list = list(new_uuids) if new_uuids else []
        cur.execute(
            """
            UPDATE users_tempmail_config
            SET processed_uuids = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = %s;
            """,
            (uuids_list, user_id)
        )
        conn.commit()
        logger.info(f"User {user_id} processed_uuids updated.")
    except Exception as e:
        logger.error(f"Error updating processed_uuids for user {user_id}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_db_connection(conn)

# --- Utility Functions ---

def create_temp_email(api_key: str):
    """Create a new temporary email address using a specific API key."""
    try:
        headers = {"Authorization": f"Bearer {api_key}"}
        response = requests.post("https://api.tempmail.co/v1/addresses", headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        email = data["data"]["email"]
        logger.info(f"📧 Created temporary email: {email}")
        return email
    except requests.RequestException as e:
        logger.error(f"❌ Error creating email with API key {api_key[:10]}...: {e}")
        return None

def get_emails(email_address: str, api_key: str):
    """Retrieve emails for the given temporary email address using a specific API key."""
    try:
        headers = {"Authorization": f"Bearer {api_key}"}
        response = requests.get(f"https://api.tempmail.co/v1/addresses/{email_address}/emails", headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        return data["data"]
    except requests.RequestException as e:
        logger.error(f"❌ Error fetching emails for {email_address} with API key {api_key[:10]}...: {e}")
        return []

def read_email(email_uuid: str, api_key: str):
    """Read the content of a specific email by its UUID using a specific API key."""
    try:
        headers = {"Authorization": f"Bearer {api_key}"}
        response = requests.get(f"https://api.tempmail.co/v1/emails/{email_uuid}", headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        return data["data"]
    except requests.RequestException as e:
        logger.error(f"❌ Error reading email {email_uuid} with API key {api_key[:10]}...: {e}")
        return None

def extract_invitation_link(email_body):
    """Extract the invitation link starting with 'https://business.facebook.com/invitation/?token='."""
    pattern = r'https://business\.facebook\.com/invitation/\?token=[^"\s]+'
    match = re.search(pattern, email_body)
    if match:
        return match.group(0)
    return None

async def wait_for_invitation_email(user_id: int, email_address: str, api_key: str, update: Update, timeout=300):
    """Wait for invitation email and extract the link, processing only new emails."""
    logger.info(f"🔄 Waiting for invitation email on: {email_address}")
    start_time = time.time()
    
    user_config = get_user_tempmail_config(user_id)
    processed_uuids = user_config.get("processed_uuids", set()) if user_config else set()

    while time.time() - start_time < timeout:
        emails = get_emails(email_address, api_key)
        new_uuids_found_in_iteration = False # Flag to check if any new emails were found in this check

        if emails:
            for email_data in emails:
                email_uuid = email_data.get('uuid')
                if email_uuid and email_uuid not in processed_uuids:
                    new_uuids_found_in_iteration = True
                    processed_uuids.add(email_uuid) # Mark as processed immediately

                    if 'from' in email_data and 'subject' in email_data:
                        if "facebook" in email_data['from'].lower() or "invitation" in email_data['subject'].lower():
                            logger.info(f"📨 Found potential invitation email from: {email_data['from']}")
                            
                            email_content = read_email(email_uuid, api_key)
                            if email_content and 'body' in email_content:
                                invitation_link = extract_invitation_link(email_content['body'])
                                if invitation_link:
                                    logger.info(f"🔗 Invitation link extracted!")
                                    # Update processed UUIDs in DB before returning
                                    update_user_processed_uuids(user_id, processed_uuids)
                                    return invitation_link
                            else:
                                logger.warning(f"⚠️ Could not read full email content or body for UUID: {email_uuid}")
                        else:
                            logger.info(f"📧 Skipping non-invitation email (from: {email_data['from']}, subject: {email_data['subject']})")
                elif email_uuid:
                    logger.debug(f"Email {email_uuid} already processed, skipping.")
        
        # Update processed UUIDs in DB if any new emails were found in this iteration
        if new_uuids_found_in_iteration:
            update_user_processed_uuids(user_id, processed_uuids)

        await send_telegram_message(update, f"⏳ Still waiting for invitation email... Checked {len(emails)} emails. Retrying in 10 seconds.", silent=True)
        time.sleep(10)  # Wait 10 seconds before checking again
    
    logger.warning("⏰ Timeout waiting for invitation email")
    return None

def generate_random_name():
    """Generate random realistic names"""
    first_names = ['Ahmed', 'Mohamed', 'Omar', 'Ali', 'Hassan', 'Mahmoud', 'Youssef', 'Khaled', 'Amr', 'Tamer', 
                   'John', 'Michael', 'David', 'James', 'Robert', 'William', 'Richard', 'Charles', 'Joseph', 'Thomas']
    last_names = ['Hassan', 'Mohamed', 'Ali', 'Ibrahim', 'Mahmoud', 'Youssef', 'Ahmed', 'Omar', 'Said', 'Farid',
                  'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
    return random.choice(first_names), random.choice(last_names)

def generate_random_email(first_name, last_name):
    """Generate random email based on name"""
    domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'protonmail.com']
    random_num = random.randint(100, 9999)
    email_formats = [
        f"{first_name.lower()}{last_name.lower()}{random_num}@{random.choice(domains)}",
        f"{first_name.lower()}{random_num}@{random.choice(domains)}",
        f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}",
        f"{first_name.lower()}{last_name.lower()}@{random.choice(domains)}",
        f"{first_name.lower()}_{last_name.lower()}{random.choice(domains)}"
    ]
    return random.choice(email_formats)

def generate_business_name():
    """Generate random business name"""
    business_prefixes = ['Tech', 'Digital', 'Smart', 'Pro', 'Elite', 'Global', 'Prime', 'Alpha', 'Meta', 'Cyber', 'Next', 'Future']
    business_suffixes = ['Solutions', 'Systems', 'Services', 'Group', 'Corp', 'Ltd', 'Inc', 'Agency', 'Studio', 'Labs', 'Works', 'Hub']
    random_num = random.randint(100, 999)
    
    name_formats = [
        f"{random.choice(business_prefixes)} {random.choice(business_suffixes)} {random_num}",
        f"{random.choice(business_prefixes)}{random_num}",
        f"M{random_num} {random.choice(business_suffixes)}",
        f"{random.choice(business_prefixes)} {random_num}",
        f"Company {random_num}" # <--- CORRECTED LINE
    ]
    return random.choice(name_formats)

def generate_random_user_agent():
    """Generate random user agent"""
    chrome_versions = ['131.0.0.0', '130.0.0.0', '129.0.0.0', '128.0.0.0']
    version = random.choice(chrome_versions)
    return f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36'

def extract_token_from_response(response):
    """Extract only DTSGInitialData token from response"""
    token_value = None
    pattern = re.compile(
        r'\["DTSGInitialData",\s*\[\],\s*\{\s*"token":\s*"([^"]+)"'
    )

    try:
        content = ""
        for chunk in response.iter_content(chunk_size=2048, decode_unicode=True):
            if chunk:
                content += chunk
                match = pattern.search(content)
                if match:
                    token_value = match.group(1)
                    return token_value

                if len(content) > 10000: # Keep only last 10000 chars to avoid memory issues
                    content = content[-10000:]

    except Exception as e:
        logger.error(f"Error reading response for token extraction: {e}")

    return token_value

def parse_cookies(cookies_input):
    """Convert cookies from text to dictionary"""
    cookies = {}
    for part in cookies_input.split(';'):
        if '=' in part:
            key, value = part.split('=', 1)
            cookies[key.strip()] = value.strip()
    return cookies

def get_user_id_from_cookies(cookies):
    """Extract user ID from cookies"""
    if 'c_user' in cookies:
        return cookies['c_user']
    logger.warning("c_user not found in cookies. Using default fallback ID.")
    return "61573547480828"  # Default fallback

# --- Helper functions for building request data/headers ---
def _get_common_headers(token_value: str, user_agent: str) -> dict:
    """Returns common headers for Facebook API requests."""
    return {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://business.facebook.com',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': user_agent,
        'x-fb-lsd': token_value,
    }

def _build_base_data(user_id: str, token_value: str) -> dict:
    """Returns common data parameters for Facebook API POST requests."""
    return {
        'av': user_id,
        '__user': user_id,
        '__a': '1',
        '__req': str(random.randint(10, 30)),
        '__hs': f'{random.randint(20000, 25000)}.BP:DEFAULT.2.0...0',
        'dpr': '1',
        '__ccg': 'MODERATE',
        '__rev': str(random.randint(1026001750, 1026001760)),
        '__s': f'{random.choice(["3arcua", "4brcub", "5csdvc"])}:{random.choice(["iccgau", "jddgbv", "kddgbv"])}:{random.choice(["myl46k", "nzm47l", "ozm48m"])}',
        '__hsi': str(random.randint(7539741099426225680, 7539741099426225690)),
        '__comet_req': '15',
        'fb_dtsg': token_value,
        'jazoest': str(random.randint(25540, 25550)),
        'lsd': token_value,
        '__spin_r': str(random.randint(1026001750, 1026001760)),
        '__spin_b': 'trunk',
        '__spin_t': str(int(time.time())),
        '__jssesw': '1',
        'server_timestamps': 'true',
    }

def _build_create_business_data(user_id: str, token_value: str, business_name: str, first_name: str, last_name: str, email: str) -> dict:
    """Builds data payload for initial business creation mutation."""
    base_data = _build_base_data(user_id, token_value)
    variables_data = {
        "input": {
            "client_mutation_id": str(random.randint(1, 999)),
            "actor_id": user_id,
            "business_name": business_name,
            "user_first_name": first_name,
            "user_last_name": last_name,
            "user_email": email,
            "creation_source": "BM_HOME_BUSINESS_CREATION_IN_SCOPE_SELECTOR",
            "entry_point": "UNIFIED_GLOBAL_SCOPE_SELECTOR"
        }
    }
    base_data.update({
        'fb_api_caller_class': 'RelayModern',
        'fb_api_req_friendly_name': 'useBusinessCreationMutationMutation',
        'variables': json.dumps(variables_data, separators=(',', ':')),
        'doc_id': '10024830640911292',
    })
    return base_data

def _build_setup_review_data(user_id: str, token_value: str, biz_id: str, admin_email: str) -> dict:
    """Builds data payload for business setup review card mutation."""
    base_data = _build_base_data(user_id, token_value)
    base_data.update({
        '__hs': '20318.BP:DEFAULT.2.0...0', # Specific for this mutation
        '__ccg': 'EXCELLENT', # Specific for this mutation
        '__rev': '1026002495', # Specific for this mutation
        '__s': 'si7vjs:iccgau:4k38c3', # Specific for this mutation
        '__hsi': '7539772710483233608', # Specific for this mutation
        '__dyn': '7xeUmxa2C5rgydwCwRyU8EKmhe2Om2q1DxiFGxK7oG484S4UKewSAAzpoixW4E726US2Sfxq4U5i4824yoyaxG4o4B0l898885G0Eo9FE4WqbwLghwLyaxBa2dmm11K6U8o2lxep122y5oeEjx63K2y1py-0DU2qwgEhxWbwQwnHxC0Bo9k2B2V8cE98451KfwXxq1-orx2ewyx6i2GU8U-U98C2i48nwCAzEowwwTxu1cwh8S1qxa3O6UW4UnwhFA0FUkx22W11wCz84e2K7EOicwVyES2e0UFU6K19xq1owqp8aE4KeyE9Eco9U4S7ErwMxN0lF9UcE3xwtU5K2G0BE88twba3i15xBxa2u1aw', # Specific for this mutation
        '__hsdp': '85v92crRg8pkFxqe5ybjyk8jdAQO5WQm2G3qm2a9StAK2KK1txC_xAC4y4KIqilJjCZKcCy8GE49BGze5ahk1exugG8ukidxe2504Fg2EadzE9UWFFEgwNqzpEb4EgG-ezFjzczF2CQA4l1gUjxK5k2d8kieFD18EYE9FE1uU5S1Gwto5q0lGl6e0dLw0X1Kq9ym2aiQezUpAximbCjw0xfw', # Specific for this mutation
        '__hblp': '0Rwbu3G6U4W1gw48w54w75xGE1oA0OA2q7pUdUCbwoobU88aWwjUdE6i1Gw8y1ZwVK0FE9U3oG1tw7jG1iw8uE3PwkU2kzonwYwrE6C1ZwiUeo1vo6i17xO4Uiy9ES6awno1kElwlEao3BwtEG3-7EhwHwmoaE9bwIAgK8x2l4xii9wwxq5GwwyEO1OyawhVE88lg4Cex-1dwSw9C3G2mi0ha0DE98iBx23a11w_xJa2W9BwBCxm2Kq4EswMyomwwwkEgwxg9Ulxm5qGqqUy2it0iUaoyazE4u2G6E4-az85e78cECayEjwrFEq_82aRwXKUSmWyGK7FWhd5gC9zdfU8rG4KE7-3zwGwfe2q68cA5A58b8gy837wxwmo2WwFwnFUc-m2O5o8oS9xWi2OnwIDwko8U8EW1wxV1C229xG7p8owNxydg6x0DwNiw8u3uui', # Specific for this mutation
        'fb_api_caller_class': 'RelayModern',
        'fb_api_req_friendly_name': 'BizKitBusinessSetupReviewCardMutation',
        'variables': f'{{"businessId":"{biz_id}","entryPoint":"BIZWEB_BIZ_SETUP_REVIEW_CARD","inviteUsers":[{{"email":"{admin_email}","roles":["ADMIN"]}}],"personalPageIdsToBeClaimed":[],"directPageUsers":[],"flowType":"BUSINESS_CREATION_IN_FBS"}}',
        'doc_id': '9845682502146236',
        'fb_api_analytics_tags': '["qpl_active_flow_ids=1001927540,558500776"]',
    })
    return base_data

# --- Core Business Creation Logic ---

async def _get_token(cookies: dict, user_agent: str, update: Update):
    """Helper to get the DTSG token."""
    headers_initial = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': user_agent,
    }

    logger.info("🔍 Getting token...")
    try:
        response = requests.get(
            'https://business.facebook.com/overview',
            cookies=cookies,
            headers=headers_initial,
            stream=True,
            timeout=30,
            allow_redirects=True
        )
        response.raise_for_status()
        
        logger.info(f"Initial request status: {response.status_code}")
        token_value = extract_token_from_response(response)
        
        if not token_value:
            await send_telegram_message(update, "❌ Token not found. Please check your cookies validity.")
            return None
        logger.info(f"✅ Token obtained successfully: {token_value[:20]}...")
        time.sleep(2)
        return token_value
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Network error during token retrieval: {e}")
        await send_telegram_message(update, f"❌ Network error during token retrieval: {escape_markdown(str(e), version=2)}", parse_mode='MarkdownV2')
        return None
    except Exception as e:
        logger.error(f"❌ General error during token retrieval: {e}")
        await send_telegram_message(update, f"❌ General error during token retrieval: {escape_markdown(str(e), version=2)}", parse_mode='MarkdownV2')
        return None

async def _create_initial_business(cookies: dict, token_value: str, user_id: str, business_name: str, first_name: str, last_name: str, email: str, user_agent: str, update: Update):
    """Helper to perform the initial business creation request."""
    headers_create = _get_common_headers(token_value, user_agent)
    headers_create['x-fb-friendly-name'] = 'useBusinessCreationMutationMutation'
    headers_create['x-asbd-id'] = str(random.randint(359340, 359350))

    data_create = _build_create_business_data(user_id, token_value, business_name, first_name, last_name, email)

    logger.info("🏢 Creating business account...")
    try:
        response_create = requests.post(
            'https://business.facebook.com/api/graphql/', 
            cookies=cookies, 
            headers=headers_create, 
            data=data_create,
            timeout=30
        )
        response_create.raise_for_status()
        
        response_text = response_create.text
        if response_text.startswith('for (;;);'):
            response_text = response_text[9:]
        
        response_json = json.loads(response_text)
        
        if 'errors' in response_json:
            for error in response_json['errors']:
                error_msg = error.get('message', '')
                if 'field_exception' in error_msg or 'حد عدد الأنشطة التجارية' in error.get('description', ''):
                    logger.warning("🛑 Facebook business creation limit reached!")
                    return None, "LIMIT_REACHED"
            
            error_messages = [error.get('message', 'Unknown error') for error in response_json['errors']]
            logger.error(f"❌ Failed to create business account: {'; '.join(error_messages)}")
            await send_telegram_message(update, f"❌ Failed to create business account: {escape_markdown('; '.join(error_messages), version=2)}", parse_mode='MarkdownV2')
            return None, f"Failed to create business account: {'; '.join(error_messages)}"
            
        elif 'error' in response_json:
            error_code = response_json.get('error', 'Unknown')
            error_desc = response_json.get('errorDescription', 'Unknown error')
            logger.error(f"❌ Error {error_code}: {error_desc}")
            await send_telegram_message(update, f"❌ Facebook API Error {error_code}: {escape_markdown(error_desc, version=2)}", parse_mode='MarkdownV2')
            return None, f"Facebook API Error {error_code}: {error_desc}"
            
        elif 'data' in response_json:
            logger.info("✅ Business account created successfully!")
            try:
                biz_id = response_json['data']['bizkit_create_business']['id']
                logger.info(f"✅ Business ID: {biz_id}")
                return biz_id, None
            except KeyError:
                logger.error("⚠️ Could not extract Business ID from response.")
                await send_telegram_message(update, "⚠️ Could not extract Business ID from response.")
                return None, "Could not extract Business ID from response."
        else:
            logger.warning("⚠️ Unexpected response format during business creation.")
            await send_telegram_message(update, "⚠️ Unexpected response format during business creation.")
            return None, "Unexpected response format during business creation."
            
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON decode error during business creation: {e}. Response: {response_create.text[:500]}...")
        await send_telegram_message(update, f"❌ JSON decode error: {escape_markdown(str(e), version=2)}", parse_mode='MarkdownV2')
        return None, f"JSON decode error: {e}"
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Network error during business creation: {e}")
        await send_telegram_message(update, f"❌ Network error during business creation: {escape_markdown(str(e), version=2)}", parse_mode='MarkdownV2')
        return None, f"Network error: {e}"
    except Exception as e:
        logger.error(f"❌ General error during business creation: {e}")
        await send_telegram_message(update, f"❌ General error during business creation: {escape_markdown(str(e), version=2)}", parse_mode='MarkdownV2')
        return None, f"General error: {e}"

async def _setup_review_and_invite(cookies: dict, token_value: str, user_id: str, biz_id: str, admin_email: str, update: Update):
    """Helper to complete business setup with review card mutation and invite admin."""
    logger.info(f"📋 Setting up business review for Business ID: {biz_id}")
    
    headers = _get_common_headers(token_value, 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36')
    headers['x-fb-friendly-name'] = 'BizKitBusinessSetupReviewCardMutation'
    headers['x-asbd-id'] = '359341' # Specific for this mutation
    headers['referer'] = 'https://business.facebook.com/billing_hub/payment_settings/?asset_id=&payment_account_id=' # Specific referer

    params = {
        '_callFlowletID': '0',
        '_triggerFlowletID': '6821',
        'qpl_active_e2e_trace_ids': '',
    }

    data = _build_setup_review_data(user_id, token_value, biz_id, admin_email)

    try:
        response = requests.post(
            'https://business.facebook.com/api/graphql/', 
            params=params, 
            cookies=cookies, 
            headers=headers, 
            data=data,
            timeout=30
        )
        response.raise_for_status()
        
        response_text = response.text
        if response_text.startswith('for (;;);'):
            response_text = response_text[9:]
        
        response_json = json.loads(response_text)
        
        if 'errors' in response_json:
            error_messages = [error.get('message', 'Unknown error') for error in response_json['errors']]
            logger.error(f"❌ Failed to complete business setup: {'; '.join(error_messages)}")
            await send_telegram_message(update, f"❌ Failed to complete business setup: {escape_markdown('; '.join(error_messages), version=2)}", parse_mode='MarkdownV2')
            return False
        elif 'error' in response_json:
            error_code = response_json.get('error', 'Unknown')
            error_desc = response_json.get('errorDescription', 'Unknown error')
            logger.error(f"❌ Setup Error {error_code}: {error_desc}")
            await send_telegram_message(update, f"❌ Setup Error {error_code}: {escape_markdown(error_desc, version=2)}", parse_mode='MarkdownV2')
            return False
        elif 'data' in response_json:
            logger.info("✅ Business setup completed successfully!")
            return True
        else:
            logger.warning(f"⚠️ Unexpected setup response format: {response_json}")
            await send_telegram_message(update, "⚠️ Unexpected setup response format.")
            return False
            
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON decode error in setup response: {e}. Response: {response.text[:500]}...")
        await send_telegram_message(update, f"❌ JSON decode error in setup: {escape_markdown(str(e), version=2)}", parse_mode='MarkdownV2')
        return False
    except requests.RequestException as e:
        logger.error(f"❌ Network error during setup: {e}")
        await send_telegram_message(update, f"❌ Network error during setup: {escape_markdown(str(e), version=2)}", parse_mode='MarkdownV2')
        return False
    except Exception as e:
        logger.error(f"❌ General error in setup: {e}")
        await send_telegram_message(update, f"❌ General error in setup: {escape_markdown(str(e), version=2)}", parse_mode='MarkdownV2')
        return False

async def create_facebook_business(cookies_dict: dict, admin_email: str, tempmail_api_key: str, update: Update):
    """
    Attempts to create a Facebook Business Manager account.
    Returns (success_status, biz_id, invitation_link, error_message)
    success_status: True for success, False for general failure, "LIMIT_REACHED" for limit.
    """
    logger.info("=== Starting Facebook Business Creation Process ===")
    
    if not cookies_dict:
        logger.error("❌ No cookies provided to the function!")
        return False, None, None, "No cookies provided."
    
    cookies = cookies_dict
    user_id = get_user_id_from_cookies(cookies)
    
    first_name, last_name = generate_random_name()
    email = generate_random_email(first_name, last_name)
    business_name = generate_business_name()
    user_agent = generate_random_user_agent()
    
    logger.info(f"\n=== Generated Data ===")
    logger.info(f"Business Name: {business_name}")
    logger.info(f"First Name: {first_name}")
    logger.info(f"Last Name: {last_name}")
    logger.info(f"Email: {email}")
    logger.info(f"Admin Email (TempMail): {admin_email}")
    logger.info(f"User ID: {user_id}")
    logger.info("=" * 30)
    
    # --- Step 1: Get Token ---
    token_value = await _get_token(cookies, user_agent, update)
    if not token_value:
        return False, None, None, "Token not found - please check cookies validity."
            
    # --- Step 2: Create Initial Business ---
    biz_id, creation_error = await _create_initial_business(
        cookies, token_value, user_id, business_name, first_name, last_name, email, user_agent, update
    )

    if creation_error == "LIMIT_REACHED":
        return "LIMIT_REACHED", None, None, "Facebook business creation limit reached."
    elif creation_error:
        return False, None, None, creation_error
    
    if not biz_id:
        return False, None, None, "Could not extract Business ID after creation."

    # --- Step 3: Setup Business Review and Invite Admin ---
    setup_success = await _setup_review_and_invite(
        cookies, token_value, user_id, biz_id, admin_email, update
    )
    
    if setup_success:
        logger.info("\n📨 Waiting for invitation email...")
        invitation_link = await wait_for_invitation_email(user_id, admin_email, tempmail_api_key, update)
        
        if invitation_link:
            return True, biz_id, invitation_link, None
        else:
            logger.warning("⚠️ Business created but no invitation link received.")
            return False, biz_id, None, "Business created but no invitation link received (TempMail issue or delay)."
    else:
        logger.warning("⚠️ Business created but setup failed.")
        return False, biz_id, None, "Business created but setup failed."

# --- Telegram Bot Functions ---

# Global variable to store user's cookies for the session (still useful for immediate access after parsing)
user_cookies_storage = {} # Stores cookies per user_id

async def send_telegram_message(update: Update, text: str, parse_mode: str = None, silent: bool = False) -> None:
    """Helper function to send messages to Telegram."""
    try:
        await update.message.reply_text(text, parse_mode=parse_mode, disable_notification=silent)
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e} - Text: {text[:100]}...")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a welcome message and prompts for cookies."""
    await send_telegram_message(update,
        "Welcome to the Facebook Business Creator Bot!\n"
        "To get started, please send your Facebook cookies as a single line of text.\n"
        "Example: `datr=...; sb=...; c_user=...; xs=...;`\n\n"
        "Then, set your TempMail API key using: `/set_tempmail_api_key YOUR_API_KEY`"
    )

async def handle_cookies_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles incoming messages, expecting them to be cookies.
    Starts the creation loop automatically if cookies are valid."""
    user_id = update.effective_user.id
    cookies_input_str = update.message.text.strip()

    if cookies_input_str:
        try:
            parsed_cookies = parse_cookies(cookies_input_str)
            if 'c_user' in parsed_cookies and 'xs' in parsed_cookies:
                user_cookies_storage[user_id] = parsed_cookies
                await send_telegram_message(update,
                    "✅ Cookies received successfully! You can now start the business creation loop.\n"
                    "If you haven't already, set your TempMail API key using: `/set_tempmail_api_key YOUR_API_KEY`\n"
                    "Then, the bot will automatically start creating businesses."
                )
                logger.info(f"User {user_id} provided valid cookies.")
                # Start the creation loop in a non-blocking way
                # We don't start it immediately here, user needs to set API key first.
                # The loop will be triggered when get_or_create_daily_temp_email is called.
                context.application.create_task(create_business_loop(update, context))

            else:
                await send_telegram_message(update,
                    "❌ Invalid cookies. Please ensure they contain at least `c_user` and `xs`."
                )
                logger.warning(f"User {user_id} provided invalid cookies format.")
        except Exception as e:
            await send_telegram_message(update, f"❌ An error occurred while parsing cookies: {escape_markdown(str(e), version=2)}\nPlease ensure the format is correct.", parse_mode='MarkdownV2')
            logger.error(f"Error parsing cookies for user {user_id}: {e}")
    else:
        await send_telegram_message(update, "❌ No cookies provided. Please send them as a single line.")
        logger.warning(f"User {user_id} sent empty message for cookies.")

async def set_tempmail_api_key(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Allows the user to set their TempMail API key."""
    user_id = update.effective_user.id
    if not context.args:
        await send_telegram_message(update, "Please provide your TempMail API key after the command. Example: `/set_tempmail_api_key YOUR_API_KEY`")
        return

    api_key = context.args[0].strip()
    
    # Check if user already exists in DB
    existing_config = get_user_tempmail_config(user_id)
    if existing_config:
        # Update existing entry
        save_user_tempmail_config(user_id, api_key, existing_config.get("current_email"), existing_config.get("email_created_date"), existing_config.get("processed_uuids"))
    else:
        # Insert new entry
        save_user_tempmail_config(user_id, api_key)

    await send_telegram_message(update, "✅ Your TempMail API key has been saved successfully! The bot will now attempt to create businesses.")
    logger.info(f"User {user_id} set their TempMail API key.")
    # Trigger the business creation loop after setting API key
    context.application.create_task(create_business_loop(update, context))


async def get_or_create_daily_temp_email(user_id: int, update: Update) -> tuple[str, str] | tuple[None, None]:
    """
    Retrieves the user's current TempMail email or creates a new one if it's a new day.
    Returns (email_address, api_key) or (None, None) if no API key is set.
    """
    user_config = get_user_tempmail_config(user_id)

    if not user_config or not user_config.get("api_key"):
        await send_telegram_message(update, "❌ Please set your TempMail API key first using `/set_tempmail_api_key YOUR_API_KEY`.")
        return None, None

    api_key = user_config["api_key"]
    current_email = user_config.get("current_email")
    email_created_date = user_config.get("email_created_date")
    
    today = datetime.now().date()

    if current_email and email_created_date == today:
        logger.info(f"User {user_id} using existing TempMail: {current_email}")
        return current_email, api_key
    else:
        logger.info(f"User {user_id}: Creating new TempMail for today.")
        new_email = create_temp_email(api_key)
        if new_email:
            # Reset processed UUIDs for the new email
            update_user_email_and_uuids(user_id, new_email, today, set())
            return new_email, api_key
        else:
            await send_telegram_message(update, "❌ Failed to create a new temporary email address. Please check your TempMail API key or try again later.")
            return None, None

async def create_business_loop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Continuously creates businesses until limit is reached or a persistent error occurs."""
    user_id = update.effective_user.id
    
    if user_id not in user_cookies_storage or not user_cookies_storage[user_id]:
        await send_telegram_message(update, "❌ Your cookies are not saved. Please send them first as a text message.")
        return

    # Get or create daily TempMail email
    admin_email, tempmail_api_key = await get_or_create_daily_temp_email(user_id, update)
    if not admin_email or not tempmail_api_key:
        return # Error message already sent by get_or_create_daily_temp_email

    business_count = 0
    while True:
        business_count += 1
        await send_telegram_message(update, f"🚀 Attempting to create Business #{business_count}...")
        logger.info(f"User {user_id}: Starting creation for Business #{business_count}")

        max_retries_per_business = 3
        initial_delay = 5 # seconds
        
        current_biz_attempt_success = False
        for attempt in range(1, max_retries_per_business + 1):
            await send_telegram_message(update, f"⏳ Business #{business_count}: Creation attempt {attempt}/{max_retries_per_business}...", silent=True)
            logger.info(f"User {user_id}: Business #{business_count}, creation attempt {attempt}")

            # Pass admin_email and tempmail_api_key to create_facebook_business
            success, biz_id, invitation_link, error_message = await create_facebook_business(
                user_cookies_storage[user_id], admin_email, tempmail_api_key, update
            )

            if success == "LIMIT_REACHED":
                await send_telegram_message(update, "🛑 Facebook business creation limit reached for these cookies! Stopping further attempts.")
                logger.info(f"User {user_id}: Business creation limit reached. Total created: {business_count - 1}")
                return # Exit the loop and function
            elif success:
                message = (
                    f"🎉 Business created successfully\\!\n"
                    f"📊 \\*Business ID:\\* `{escape_markdown(biz_id, version=2)}`\n"
                    f"🔗 \\*Invitation Link:\\* {escape_markdown(invitation_link, version=2)}"
                )
                await send_telegram_message(update, message, parse_mode='MarkdownV2')
                logger.info(f"User {user_id}: Business #{business_count} created successfully on attempt {attempt}.")
                current_biz_attempt_success = True
                break # Break from inner retry loop, move to next business
            else:
                logger.error(f"User {user_id}: Business #{business_count} creation failed on attempt {attempt}. Reason: {error_message}")
                
                if attempt < max_retries_per_business:
                    delay = initial_delay * (2 ** (attempt - 1)) # Exponential backoff
                    await send_telegram_message(update, 
                        f"❌ Business #{business_count}: Creation failed on attempt {attempt}. Reason: {escape_markdown(error_message, version=2)}\n"
                        f"Retrying in {delay} seconds...", parse_mode='MarkdownV2'
                    )
                    time.sleep(delay)
                else:
                    final_error_message = (
                        f"❌ Business #{business_count}: All {max_retries_per_business} attempts failed.\n"
                        f"Last error: {escape_markdown(error_message, version=2)}"
                    )
                    if biz_id:
                        final_error_message += f"\n📊 \\*Partial Business ID:\\* `{escape_markdown(biz_id, version=2)}`"
                    await send_telegram_message(update, final_error_message, parse_mode='MarkdownV2')
                    logger.error(f"User {user_id}: Business #{business_count}: All attempts failed. Final error: {error_message}")
        
        if not current_biz_attempt_success:
            await send_telegram_message(update, f"⚠️ Business #{business_count} could not be created after multiple retries. Moving to next business attempt.")
            time.sleep(random.randint(10, 20))
        else:
            await send_telegram_message(update, f"✅ Business #{business_count} created. Waiting a bit before next attempt...")
            time.sleep(random.randint(5, 15))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a help message."""
    await send_telegram_message(update,
        "I am a bot for creating Facebook Business Manager accounts.\n\n"
        "Steps:\n"
        "1. Send me your Facebook cookies as a single line of text.\n"
        "2. Set your TempMail API key using: `/set_tempmail_api_key YOUR_API_KEY`\n"
        "3. I will automatically start creating businesses for you until the limit is reached.\n\n"
        "Note: The process might take a few minutes per business and includes retries for robustness."
    )

async def error_handler(update: object, context: CallbackContext) -> None:
    """Log the error and send a telegram message to notify the user."""
    logger.error("Exception while handling an update:", exc_info=context.error)
    
    tb_string = "".join(traceback.format_exception(None, context.error, context.error.__traceback__))
    logger.error(f"Full traceback:\n{tb_string}")

    escaped_error_message = escape_markdown(str(context.error), version=2)
    
    message = (
        "An unexpected error occurred while processing your request\\. "
        "The developers have been notified\\.\n\n"
        f"Error: `{escaped_error_message}`"
    )
    
    if update.effective_message:
        await send_telegram_message(update, message, parse_mode='MarkdownV2')
    else:
        logger.warning("Error handler called without an effective message.")

def main_telegram_bot():
    """Starts the Telegram bot."""
    logger.info("Starting Telegram Bot...")
    
    # Initialize database schema
    init_db() 

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("set_tempmail_api_key", set_tempmail_api_key)) # New command

    # Message handler for cookies (any text message that is not a command)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_cookies_message))

    # Register error handler
    application.add_error_handler(error_handler) 

    logger.info("Bot is running. Press Ctrl+C to stop.")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main_telegram_bot()

