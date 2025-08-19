import re
import requests
import random
import json
import string
import time
import logging
import traceback
from datetime import datetime, timedelta, date
import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import psycopg2.pool

# Telegram Bot API imports
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackContext, CallbackQueryHandler
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
ADMIN_IDS_STR = os.environ.get('ADMIN_IDS', '5895491379')
ADMIN_IDS = [int(admin_id.strip()) for admin_id in ADMIN_IDS_STR.split(',') if admin_id.strip().isdigit()]

if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable not set!")
    exit(1)
if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable not set!")
    exit(1)

# --- Global Variables ---
# Stores user's cookies for the current session (in-memory, not persistent)
user_cookies_storage = {} 
# Stores the current running task for each user to manage start/stop
user_tasks = {} 

# --- Database Connection Pool ---
conn_pool = None

def get_db_connection():
    """Establishes and returns a database connection from the pool."""
    global conn_pool
    if conn_pool is None:
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
        
        # Function to update 'updated_at' column
        cur.execute("""
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)

        # Table for users and their TempMail config, subscription, and admin status
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                is_admin BOOLEAN DEFAULT FALSE,
                subscription_end_date DATE,
                tempmail_api_key TEXT,
                current_email TEXT,
                email_created_at DATE,
                processed_uuids TEXT[] DEFAULT '{}',
                businesses_created_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'idle', -- idle, running, paused
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            DROP TRIGGER IF EXISTS update_users_updated_at ON users;
            CREATE TRIGGER update_users_updated_at
            BEFORE UPDATE ON users
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        """)

        # Table for storing created businesses and their invitation links
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_businesses (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                biz_id TEXT NOT NULL,
                invitation_link TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
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

# --- Database Operations for Users ---

def get_user(user_id: int) -> dict | None:
    """Retrieves user data from the database."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT user_id, is_admin, subscription_end_date, tempmail_api_key, current_email, email_created_at, processed_uuids, businesses_created_count, status FROM users WHERE user_id = %s",
            (user_id,)
        )
        row = cur.fetchone()
        if row:
            return {
                "user_id": row[0],
                "is_admin": row[1],
                "subscription_end_date": row[2],
                "tempmail_api_key": row[3],
                "current_email": row[4],
                "email_created_at": row[5],
                "processed_uuids": set(row[6]) if row[6] else set(),
                "businesses_created_count": row[7],
                "status": row[8]
            }
        return None
    except Exception as e:
        logger.error(f"Error getting user {user_id}: {e}")
        return None
    finally:
        if conn:
            release_db_connection(conn)

def create_or_update_user(user_id: int, is_admin: bool = False, subscription_end_date: date = None, tempmail_api_key: str = None, current_email: str = None, email_created_at: date = None, processed_uuids: set = None, businesses_created_count: int = 0, status: str = 'idle'):
    """Creates or updates user data in the database."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Ensure processed_uuids is a list for PostgreSQL array
        uuids_list = list(processed_uuids) if processed_uuids else []

        cur.execute(
            """
            INSERT INTO users (user_id, is_admin, subscription_end_date, tempmail_api_key, current_email, email_created_at, processed_uuids, businesses_created_count, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                is_admin = EXCLUDED.is_admin,
                subscription_end_date = EXCLUDED.subscription_end_date,
                tempmail_api_key = COALESCE(EXCLUDED.tempmail_api_key, users.tempmail_api_key), -- Only update if EXCLUDED is not NULL
                current_email = COALESCE(EXCLUDED.current_email, users.current_email),
                email_created_at = COALESCE(EXCLUDED.email_created_at, users.email_created_at),
                processed_uuids = COALESCE(EXCLUDED.processed_uuids, users.processed_uuids),
                businesses_created_count = EXCLUDED.businesses_created_count,
                status = EXCLUDED.status,
                updated_at = CURRENT_TIMESTAMP;
            """,
            (user_id, is_admin, subscription_end_date, tempmail_api_key, current_email, email_created_at, uuids_list, businesses_created_count, status)
        )
        conn.commit()
        logger.info(f"User {user_id} data saved/updated.")
    except Exception as e:
        logger.error(f"Error saving/updating user {user_id}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_db_connection(conn)

def update_user_status(user_id: int, status: str):
    """Updates only the status of a user."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE user_id = %s",
            (status, user_id)
        )
        conn.commit()
        logger.info(f"User {user_id} status updated to {status}.")
    except Exception as e:
        logger.error(f"Error updating user {user_id} status: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_db_connection(conn)

def update_user_tempmail_config(user_id: int, api_key: str = None, email: str = None, email_date: date = None, uuids: set = None):
    """Updates TempMail related config for a user."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        set_clauses = []
        params = []

        if api_key is not None:
            set_clauses.append("tempmail_api_key = %s")
            params.append(api_key)
        if email is not None:
            set_clauses.append("current_email = %s")
            params.append(email)
        if email_date is not None:
            set_clauses.append("email_created_at = %s")
            params.append(email_date)
        if uuids is not None:
            set_clauses.append("processed_uuids = %s")
            params.append(list(uuids)) # Convert set to list for PostgreSQL array
        
        if not set_clauses:
            return # Nothing to update

        set_clauses.append("updated_at = CURRENT_TIMESTAMP")
        params.append(user_id)

        query = f"UPDATE users SET {', '.join(set_clauses)} WHERE user_id = %s"
        cur.execute(query, params)
        conn.commit()
        logger.info(f"User {user_id} TempMail config updated.")
    except Exception as e:
        logger.error(f"Error updating TempMail config for user {user_id}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_db_connection(conn)

def increment_businesses_created_count(user_id: int):
    """Increments the businesses_created_count for a user."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET businesses_created_count = businesses_created_count + 1, updated_at = CURRENT_TIMESTAMP WHERE user_id = %s",
            (user_id,)
        )
        conn.commit()
        logger.info(f"User {user_id} businesses_created_count incremented.")
    except Exception as e:
        logger.error(f"Error incrementing businesses_created_count for user {user_id}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_db_connection(conn)

def get_all_users() -> list[dict]:
    """Retrieves all users from the database."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT user_id, is_admin, subscription_end_date, businesses_created_count, status, created_at FROM users ORDER BY created_at DESC"
        )
        users_data = []
        for row in cur.fetchall():
            users_data.append({
                "user_id": row[0],
                "is_admin": row[1],
                "subscription_end_date": row[2],
                "businesses_created_count": row[3],
                "status": row[4],
                "created_at": row[5]
            })
        return users_data
    except Exception as e:
        logger.error(f"Error getting all users: {e}")
        return []
    finally:
        if conn:
            release_db_connection(conn)

# --- Database Operations for User Businesses ---

def save_user_business(user_id: int, biz_id: str, invitation_link: str):
    """Saves a created business's details for a user."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO user_businesses (user_id, biz_id, invitation_link) VALUES (%s, %s, %s)",
            (user_id, biz_id, invitation_link)
        )
        conn.commit()
        logger.info(f"Business {biz_id} saved for user {user_id}.")
    except Exception as e:
        logger.error(f"Error saving business {biz_id} for user {user_id}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_db_connection(conn)

def get_user_businesses(user_id: int) -> list[dict]:
    """Retrieves all businesses created by a specific user."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT biz_id, invitation_link, created_at FROM user_businesses WHERE user_id = %s ORDER BY created_at DESC",
            (user_id,)
        )
        businesses = []
        for row in cur.fetchall():
            businesses.append({
                "biz_id": row[0],
                "invitation_link": row[1],
                "created_at": row[2]
            })
        return businesses
    except Exception as e:
        logger.error(f"Error getting businesses for user {user_id}: {e}")
        return []
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
    
    user_data = get_user(user_id)
    processed_uuids = user_data.get("processed_uuids", set()) if user_data else set()

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
                                    update_user_tempmail_config(user_id, uuids=processed_uuids)
                                    return invitation_link
                            else:
                                logger.warning(f"⚠️ Could not read full email content or body for UUID: {email_uuid}")
                        else:
                            logger.info(f"📧 Skipping non-invitation email (from: {email_data['from']}, subject: {email_data['subject']})")
                elif email_uuid:
                    logger.debug(f"Email {email_uuid} already processed, skipping.")
        
        # Update processed UUIDs in DB if any new emails were found in this iteration
        if new_uuids_found_in_iteration:
            update_user_tempmail_config(user_id, uuids=processed_uuids)

        await send_telegram_message(update, "⏳ Still waiting for invitation email... Retrying in 10 seconds.", silent=True)
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
        f"{first_name.lower()}_{last_name.lower()}@{random.choice(domains)}"
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
        f"Company {random_num}"
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
            await send_telegram_message(update, "❌ Token not found\\. Please check your cookies validity\\.", parse_mode='MarkdownV2')
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

async def _create_initial_business(
    cookies: dict, 
    token_value: str, 
    user_id: str, 
    business_name: str, 
    first_name: str, 
    last_name: str, 
    email: str, 
    user_agent: str, 
    update: Update
):
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
            await send_telegram_message(
                update, 
                f"❌ *فشل إنشاء حساب البيزنس:*\n{escape_markdown('; '.join(error_messages), version=2)}", 
                parse_mode='MarkdownV2'
            )
            return None, f"Failed to create business account: {'; '.join(error_messages)}"
            
        elif 'error' in response_json:
            error_code = response_json.get('error', 'Unknown')
            error_desc = response_json.get('errorDescription', 'Unknown error')
            logger.error(f"❌ Error {error_code}: {error_desc}")
            await send_telegram_message(
                update, 
                f"❌ *Facebook API Error {error_code}:*\n{escape_markdown(error_desc, version=2)}", 
                parse_mode='MarkdownV2'
            )
            return None, f"Facebook API Error {error_code}: {error_desc}"
            
        elif 'data' in response_json:
            logger.info("✅ Business account created successfully!")
            try:
                biz_id = response_json['data']['bizkit_create_business']['id']
                logger.info(f"✅ Business ID: {biz_id}")
                await send_telegram_message(
                    update,
                    f"✅ *تم إنشاء حساب بيزنس جديد بنجاح!*\n\n🆔 *Business ID:* `{biz_id}`",
                    parse_mode='MarkdownV2'
                )
                return biz_id, None
            except KeyError:
                logger.error("⚠️ Could not extract Business ID from response.")
                await send_telegram_message(update, "⚠️ *لم أستطع استخراج الـ Business ID من الاستجابة*", parse_mode='MarkdownV2')
                return None, "Could not extract Business ID from response."
        else:
            logger.warning("⚠️ Unexpected response format during business creation.")
            await send_telegram_message(update, "⚠️ *Unexpected response format أثناء إنشاء الحساب*", parse_mode='MarkdownV2')
            return None, "Unexpected response format during business creation."
            
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON decode error during business creation: {e}. Response: {response_create.text[:500]}...")
        await send_telegram_message(update, f"❌ *JSON decode error:*\n{escape_markdown(str(e), version=2)}", parse_mode='MarkdownV2')
        return None, f"JSON decode error: {e}"
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Network error during business creation: {e}")
        await send_telegram_message(update, f"❌ *Network error:*\n{escape_markdown(str(e), version=2)}", parse_mode='MarkdownV2')
        return None, f"Network error: {e}"
    except Exception as e:
        logger.error(f"❌ General error during business creation: {e}")
        await send_telegram_message(update, f"❌ *General error:*\n{escape_markdown(str(e), version=2)}", parse_mode='MarkdownV2')
        return None, f"General error: {e}"

async def _setup_review_and_invite(cookies: dict, token_value: str, user_id: str, biz_id: str, admin_email: str, update: Update):
    """Helper to complete business setup with review card mutation and invite admin."""
    logger.info(f"📋 Setting up business review for Business ID: {biz_id}")
    
    headers = _get_common_headers(token_value, 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36')
    headers['x-fb-friendly-name'] = 'BizKitBusinessSetupReviewCardMutation'
    headers['x-asbd-id'] = '359341'  # Specific for this mutation
    headers['referer'] = 'https://business.facebook.com/billing_hub/payment_settings/?asset_id=&payment_account_id='  # Specific referer

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
            await send_telegram_message(
                update, 
                f"❌ *Failed to complete business setup*\n\n🔻 {escape_markdown('; '.join(error_messages), version=2)}", 
                parse_mode='MarkdownV2'
            )
            return False

        elif 'error' in response_json:
            error_code = response_json.get('error', 'Unknown')
            error_desc = response_json.get('errorDescription', 'Unknown error')
            logger.error(f"❌ Setup Error {error_code}: {error_desc}")
            await send_telegram_message(
                update, 
                f"❌ *Setup Error {escape_markdown(str(error_code), version=2)}*\n\n🔻 {escape_markdown(error_desc, version=2)}", 
                parse_mode='MarkdownV2'
            )
            return False

        elif 'data' in response_json:
            logger.info("✅ Business setup completed successfully!")
            await send_telegram_message(
                update, 
                f"✅ *Business setup completed successfully* for Biz ID `{escape_markdown(biz_id, version=2)}`", 
                parse_mode='MarkdownV2'
            )
            return True

        else:
            logger.warning(f"⚠️ Unexpected setup response format: {response_json}")
            await send_telegram_message(update, "⚠️ *Unexpected setup response format*", parse_mode='MarkdownV2')
            return False
            
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON decode error in setup response: {e}. Response: {response.text[:500]}...")
        await send_telegram_message(
            update, 
            f"❌ *JSON decode error in setup*\n\n🔻 {escape_markdown(str(e), version=2)}", 
            parse_mode='MarkdownV2'
        )
        return False

    except requests.RequestException as e:
        logger.error(f"❌ Network error during setup: {e}")
        await send_telegram_message(
            update, 
            f"❌ *Network error during setup*\n\n🔻 {escape_markdown(str(e), version=2)}", 
            parse_mode='MarkdownV2'
        )
        return False

    except Exception as e:
        logger.error(f"❌ General error in setup: {e}")
        await send_telegram_message(
            update, 
            f"❌ *General error in setup*\n\n🔻 {escape_markdown(str(e), version=2)}", 
            parse_mode='MarkdownV2'
        )
        return False

async def create_facebook_business(cookies_dict: dict, admin_email: str, tempmail_api_key: str, update: Update):
    """
    Attempts to create a Facebook Business Manager account.
    Returns (success_status, biz_id, invitation_link, error_message)
    success_status: 
        - True → success
        - False → failure
        - "LIMIT_REACHED" → limit reached.
    """
    logger.info("=== Starting Facebook Business Creation Process ===")

    if not cookies_dict:
        logger.error("❌ No cookies provided to the function!")
        return False, None, None, "No cookies provided."

    # Cookies & FB user ID
    cookies = cookies_dict
    user_id_from_cookies = get_user_id_from_cookies(cookies)  # FB user ID, not Telegram

    # --- Pre-check cookies validity ---
    try:
        await send_telegram_message(update, "🔍 Checking cookies validity...", silent=True)
        response = requests.get(
            "https://business.facebook.com/overview",
            cookies=cookies,
            timeout=15,
            allow_redirects=True
        )
        response.raise_for_status()

        if "login.php" in response.url:
            await send_telegram_message(
                update,
                "❌ Your cookies are invalid or expired\\. Please send new cookies\\.",
                parse_mode="MarkdownV2"
            )
            return False, None, None, "Cookies invalid or expired."

        await send_telegram_message(
            update,
            "✅ Cookies appear valid\\.",
            silent=True,
            parse_mode="MarkdownV2"
        )

    except requests.RequestException as e:
        await send_telegram_message(
            update,
            f"❌ Failed to validate cookies: {escape_markdown(str(e), version=2)}\\. Please send new cookies\\.",
            parse_mode="MarkdownV2"
        )
        return False, None, None, f"Failed to validate cookies: {e}"

    # --- Generate fake data ---
    first_name, last_name = generate_random_name()
    email = generate_random_email(first_name, last_name)
    business_name = generate_business_name()
    user_agent = generate_random_user_agent()

    logger.info("\n=== Generated Data ===")
    logger.info(f"Business Name: {business_name}")
    logger.info(f"First Name: {first_name}")
    logger.info(f"Last Name: {last_name}")
    logger.info(f"Email: {email}")
    logger.info(f"Admin Email (TempMail): {admin_email}")
    logger.info(f"User ID (from cookies): {user_id_from_cookies}")
    logger.info("=" * 30)

    # --- Step 1: Get Token ---
    token_value = await _get_token(cookies, user_agent, update)
    if not token_value:
        return False, None, None, "Token not found - please check cookies validity."

    # --- Step 2: Create Initial Business ---
    biz_id, creation_error = await _create_initial_business(
        cookies, token_value, user_id_from_cookies,
        business_name, first_name, last_name, email,
        user_agent, update
    )

    if creation_error == "LIMIT_REACHED":
        return "LIMIT_REACHED", None, None, "Facebook business creation limit reached."
    elif creation_error:
        return False, None, None, creation_error

    if not biz_id:
        return False, None, None, "Could not extract Business ID after creation."

    # --- Step 3: Setup Business Review + Invite Admin ---
    setup_success = await _setup_review_and_invite(
        cookies, token_value, user_id_from_cookies,
        biz_id, admin_email, update
    )

    if setup_success:
        logger.info("\n📨 Waiting for invitation email...")
        invitation_link = await wait_for_invitation_email(
            update.effective_user.id,
            admin_email,
            tempmail_api_key,
            update
        )

        if invitation_link:
            return True, biz_id, invitation_link, None
        else:
            logger.warning("⚠️ Business created but no invitation link received.")
            return False, biz_id, None, (
                "Business created but no invitation link received (TempMail issue or delay)."
            )
    else:
        logger.warning("⚠️ Business created but setup failed.")
        return False, biz_id, None, "Business created but setup failed."

# Global variable to store user's cookies for the current session (in-memory, not persistent)
user_cookies_storage = {} 
# Stores the current running task for each user to manage start/stop
user_tasks = {} 

async def send_telegram_message(update: Update, text: str, parse_mode: str = None, reply_markup: InlineKeyboardMarkup = None, silent: bool = False) -> None:
    """Helper function to send messages to Telegram."""
    try:
        # Ensure text is escaped if MarkdownV2 is used
        if parse_mode == 'MarkdownV2':
            text = escape_markdown(text, version=2)
        
        if update.callback_query:
            await update.callback_query.message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup, disable_notification=silent)
        else:
            await update.message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup, disable_notification=silent)
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e} - Text: {text[:100]}...")

async def get_main_keyboard(user_id: int, user_status: str = 'idle') -> InlineKeyboardMarkup:
    """Returns the main keyboard for users based on their status."""
    keyboard = []
    if user_status == 'idle' or user_status == 'paused':
        keyboard.append([InlineKeyboardButton("🚀 Start New Session", callback_data="start_creation")])
    if user_status == 'running':
        keyboard.append([InlineKeyboardButton("⏸️ Pause", callback_data="pause_creation")])
    if user_status == 'paused':
        keyboard.append([InlineKeyboardButton("▶️ Resume", callback_data="resume_creation")])
    
    keyboard.append([
        InlineKeyboardButton("⬇️ Download Invitations", callback_data="download_invitations"),
        InlineKeyboardButton("🆔 Download IDs", callback_data="download_ids")
    ])
    keyboard.append([InlineKeyboardButton("🔄 Refresh Panel", callback_data="refresh_panel")])

    # Add Admin Panel button if user is an admin
    user_data = get_user(user_id)
    if user_data and user_data.get('is_admin'):
        keyboard.append([InlineKeyboardButton("⚙️ Admin Panel", callback_data="show_admin_panel")])

    return InlineKeyboardMarkup(keyboard)

async def send_user_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends the user's main control panel."""
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    
    if not user_data:
        # This should ideally not happen if create_or_update_user is called on /start
        # but as a fallback, create a basic entry
        create_or_update_user(user_id)
        user_data = get_user(user_id) # Re-fetch to get default values

    status_text = {
        'idle': 'Idle',
        'running': 'Running',
        'paused': 'Paused'
    }.get(user_data['status'], 'Unknown')

    sub_status = "Inactive"
    if user_data['subscription_end_date']:
        if user_data['subscription_end_date'] >= date.today():
            days_left = (user_data['subscription_end_date'] - date.today()).days
            sub_status = f"Active until {user_data['subscription_end_date'].strftime('%Y-%m-%d')} \\({days_left} days left\\)"
        else:
            sub_status = "Expired"

    message_text = (
        f"👋 Welcome To Your MY Bot \n\n"
        f"✨ Subscription Status:{sub_status}\n"
        f"📧 Your Daily TempMail:`{user_data['current_email'] or 'Not set'}`\n"
        f"📊 Businesses Created:`{user_data['businesses_created_count']}`\n"
        f"⚙️ Bot Status:`{status_text}`\n\n"
        f"Please send your Facebook cookies as a text message to start working\\."
    )
    
    await send_telegram_message(update, message_text, parse_mode='MarkdownV2', reply_markup=await get_main_keyboard(user_id, user_data['status']))

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a welcome message and the user panel."""
    user_id = update.effective_user.id
    # Ensure user exists in DB and set admin status if applicable
    user_data = get_user(user_id)
    if not user_data:
        is_admin_status = user_id in ADMIN_IDS
        create_or_update_user(user_id, is_admin=is_admin_status)
        if is_admin_status:
            logger.info(f"User {user_id} set as admin upon start.")
    else: # If user exists, ensure admin status is up-to-date
        if user_data['is_admin'] != (user_id in ADMIN_IDS):
            create_or_update_user(user_id, is_admin=(user_id in ADMIN_IDS))
            logger.info(f"User {user_id} admin status updated.")

    await send_user_panel(update, context)

async def handle_cookies_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles incoming messages, expecting them to be cookies."""
    user_id = update.effective_user.id
    cookies_input_str = update.message.text.strip()

    if cookies_input_str:
        try:
            parsed_cookies = parse_cookies(cookies_input_str)
            if 'c_user' in parsed_cookies and 'xs' in parsed_cookies:
                user_cookies_storage[user_id] = parsed_cookies
                await send_telegram_message(update, "✅ Cookies received successfully\\! You can now start the business creation process\\.", parse_mode='MarkdownV2')
                logger.info(f"User {user_id} provided valid cookies.")
                await send_user_panel(update, context) # Refresh panel
            else:
                await send_telegram_message(update, "❌ Invalid cookies\\. Please ensure they contain at least `c_user` and `xs`\\.", parse_mode='MarkdownV2')
                logger.warning(f"User {user_id} provided invalid cookies format.")
        except Exception as e:
            await send_telegram_message(update, f"❌ An error occurred while parsing cookies: {escape_markdown(str(e), version=2)}\\. Please ensure the format is correct\\.", parse_mode='MarkdownV2')
            logger.error(f"Error parsing cookies for user {user_id}: {e}")
    else:
        await send_telegram_message(update, "❌ No cookies provided\\. Please send them as a single line\\.", parse_mode='MarkdownV2')
        logger.warning(f"User {user_id} sent empty message for cookies.")

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles button presses from inline keyboards."""
    query = update.callback_query
    await query.answer() # Acknowledge the button press

    user_id = query.from_user.id
    user_data = get_user(user_id)

    if not user_data:
        await send_telegram_message(update, "Please start the bot first using the /start command\\.", parse_mode='MarkdownV2')
        return

    action = query.data

    if action == "start_creation":
        if user_data['status'] == 'running':
            await send_telegram_message(update, "Bot is already running\\.", parse_mode='MarkdownV2')
            return
        
        if not user_data['subscription_end_date'] or user_data['subscription_end_date'] < date.today():
            await send_telegram_message(update, "❌ Your subscription is inactive or expired\\. Please contact the admin to activate it\\.", parse_mode='MarkdownV2')
            return

        if user_id not in user_cookies_storage or not user_cookies_storage[user_id]:
            await send_telegram_message(update, "❌ Your cookies are not saved\\. Please send them first as a text message\\.", parse_mode='MarkdownV2')
            return

        update_user_status(user_id, 'running')
        await send_telegram_message(update, "🚀 Starting business creation process\\.", parse_mode='MarkdownV2', reply_markup=await get_main_keyboard(user_id, 'running'))
        # Start the creation loop in a non-blocking way
        task = context.application.create_task(create_business_loop(update, context))
        user_tasks[user_id] = task # Store the task to be able to cancel it
        
    elif action == "pause_creation":
        if user_data['status'] != 'running':
            await send_telegram_message(update, "Bot is not running to be paused\\.", parse_mode='MarkdownV2')
            return
        
        update_user_status(user_id, 'paused')
        if user_id in user_tasks and not user_tasks[user_id].done():
            user_tasks[user_id].cancel()
            await send_telegram_message(update, "⏸️ Business creation process paused\\.", parse_mode='MarkdownV2', reply_markup=await get_main_keyboard(user_id, 'paused'))
        else:
            await send_telegram_message(update, "Bot is not actively running to be paused\\.", parse_mode='MarkdownV2', reply_markup=await get_main_keyboard(user_id, 'paused'))

    elif action == "resume_creation":
        if user_data['status'] != 'paused':
            await send_telegram_message(update, "Bot is not paused to be resumed\\.", parse_mode='MarkdownV2')
            return
        
        update_user_status(user_id, 'running')
        await send_telegram_message(update, "▶️ Resuming business creation process\\.", parse_mode='MarkdownV2', reply_markup=await get_main_keyboard(user_id, 'running'))
        task = context.application.create_task(create_business_loop(update, context))
        user_tasks[user_id] = task

    elif action == "download_invitations":
        businesses = get_user_businesses(user_id)
        if not businesses:
            await send_telegram_message(update, "No invitations available for download yet\\.", parse_mode='MarkdownV2')
            return
        
        file_content = "Business ID,Invitation Link,Created At\n"
        for biz in businesses:
            file_content += f"{biz['biz_id']},{biz['invitation_link']},{biz['created_at'].strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        file_name = f"invitations_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        await context.bot.send_document(chat_id=user_id, document=file_content.encode('utf-8'), filename=file_name)
        await send_telegram_message(update, "✅ Invitations file sent successfully\\.", parse_mode='MarkdownV2')

    elif action == "download_ids":
        businesses = get_user_businesses(user_id)
        if not businesses:
            await send_telegram_message(update, "No business IDs available for download yet\\.", parse_mode='MarkdownV2')
            return
        
        file_content = "Business ID,Created At\n"
        for biz in businesses:
            file_content += f"{biz['biz_id']},{biz['created_at'].strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        file_name = f"business_ids_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        await context.bot.send_document(chat_id=user_id, document=file_content.encode('utf-8'), filename=file_name)
        await send_telegram_message(update, "✅ Business IDs file sent successfully\\.", parse_mode='MarkdownV2')

    elif action == "refresh_panel":
        await send_user_panel(update, context)
    
    elif action == "show_admin_panel":
        await send_admin_panel(update, context)


async def get_or_create_daily_temp_email(user_id: int, update: Update) -> tuple[str, str] | tuple[None, None]:
    """
    Retrieves the user's current TempMail email or creates a new one if it's a new day.
    Returns (email_address, api_key) or (None, None) if no API key is set.
    """
    user_data = get_user(user_id)

    if not user_data or not user_data.get("tempmail_api_key"):
        await send_telegram_message(update, "❌ Please set your TempMail API key first\\. Contact the admin or use the /set_tempmail_api_key command if available to you\\.", parse_mode='MarkdownV2')
        return None, None

    api_key = user_data["tempmail_api_key"]
    current_email = user_data.get("current_email")
    email_created_date = user_data.get("email_created_at")
    
    today = date.today()

    if current_email and email_created_date == today:
        logger.info(f"User {user_id} using existing TempMail: {current_email}")
        return current_email, api_key
    else:
        logger.info(f"User {user_id}: Creating new TempMail for today.")
        new_email = create_temp_email(api_key)
        if new_email:
            # Reset processed UUIDs for the new email
            update_user_tempmail_config(user_id, email=new_email, email_date=today, uuids=set())
            return new_email, api_key
        else:
            await send_telegram_message(update, "❌ Failed to create a new temporary email address\\. Please check your TempMail API key or try again later\\.", parse_mode='MarkdownV2')
            return None, None

async def create_business_loop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Continuously creates businesses until limit is reached or a persistent error occurs."""
    user_id = update.effective_user.id
    
    # Check user status from DB
    user_data = get_user(user_id)
    if not user_data or user_data['status'] != 'running':
        logger.info(f"User {user_id} is not in 'running' status. Stopping loop.")
        return # Stop if not explicitly running

    if user_id not in user_cookies_storage or not user_cookies_storage[user_id]:
        await send_telegram_message(
            update,
            "❌ *Your cookies are not saved!*\n\n📌 Please send them first as a text message.",
            parse_mode='MarkdownV2'
        )
        update_user_status(user_id, 'idle')
        await send_user_panel(update, context)
        return

    # Get or create daily TempMail email
    admin_email, tempmail_api_key = await get_or_create_daily_temp_email(user_id, update)
    if not admin_email or not tempmail_api_key:
        update_user_status(user_id, 'idle')
        await send_user_panel(update, context)
        return # Error message already sent by get_or_create_daily_temp_email

    while True:
        # Re-check status in each iteration
        user_data = get_user(user_id)
        if not user_data or user_data['status'] != 'running':
            logger.info(f"User {user_id} changed status to {user_data['status']}. Stopping loop.")
            break # Exit the loop if status is not 'running'

        # Check subscription validity
        if not user_data['subscription_end_date'] or user_data['subscription_end_date'] < date.today():
            await send_telegram_message(
                update,
                "❌ *Your subscription has expired!*\n\nBusiness creation process has been stopped.",
                parse_mode='MarkdownV2'
            )
            update_user_status(user_id, 'idle')
            await send_user_panel(update, context)
            break # Exit the loop

        current_businesses_count = user_data['businesses_created_count'] + 1
        await send_telegram_message(
            update,
            f"🚀 *Starting creation for Business \\#{current_businesses_count}...*",
            parse_mode='MarkdownV2'
        )
        logger.info(f"User {user_id}: Starting creation for Business #{current_businesses_count}")

        max_retries_per_business = 3
        initial_delay = 5 # seconds
        
        current_biz_attempt_success = False
        for attempt in range(1, max_retries_per_business + 1):
            await send_telegram_message(
                update,
                f"⏳ Business \\#{current_businesses_count}: *Attempt {attempt}/{max_retries_per_business}...*",
                parse_mode='MarkdownV2',
                silent=True
            )
            logger.info(f"User {user_id}: Business #{current_businesses_count}, creation attempt {attempt}")

            success, biz_id, invitation_link, error_message = await create_facebook_business(
                user_cookies_storage[user_id], admin_email, tempmail_api_key, update
            )

            if success == "LIMIT_REACHED":
                await send_telegram_message(
                    update,
                    "🛑 *Facebook business creation limit reached for these cookies!*\n\nNo more attempts will be made.",
                    parse_mode='MarkdownV2'
                )
                logger.info(f"User {user_id}: Business creation limit reached. Total created: {user_data['businesses_created_count']}")
                update_user_status(user_id, 'idle')
                await send_user_panel(update, context)
                return # Exit the loop and function

            elif success:
                save_user_business(user_id, biz_id, invitation_link)
                increment_businesses_created_count(user_id)
                
                message = (
                    "🎉 *Business Created Successfully!*\n\n"
                    f"📊 *Business ID:* `{escape_markdown(biz_id, version=2)}`\n"
                    f"🔗 *Invitation Link:*"
                )

                keyboard = [
                    [InlineKeyboardButton("🔗 Open Invitation", url=invitation_link)],
                    [InlineKeyboardButton("📋 Copy ID", callback_data=f"copy_biz_id_{biz_id}")]
                ]

                await send_telegram_message(
                    update,
                    message,
                    parse_mode='MarkdownV2',
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                logger.info(f"User {user_id}: Business #{current_businesses_count} created successfully on attempt {attempt}.")
                current_biz_attempt_success = True
                break # Break from inner retry loop, move to next business

            else:
                logger.error(f"User {user_id}: Business #{current_businesses_count} creation failed on attempt {attempt}. Reason: {error_message}")
                
                if attempt < max_retries_per_business:
                    delay = initial_delay * (2 ** (attempt - 1)) # Exponential backoff
                    await send_telegram_message(
                        update, 
                        f"❌ Business \\#{current_businesses_count}: *Attempt {attempt} failed!*\n"
                        f"📌 Reason: `{escape_markdown(error_message, version=2)}`\n\n"
                        f"⏳ Retrying in {delay} seconds...",
                        parse_mode='MarkdownV2'
                    )
                    time.sleep(delay)
                else:
                    final_error_message = (
                        f"❌ Business \\#{current_businesses_count}: *All {max_retries_per_business} attempts failed!*\n\n"
                        f"📌 Last error: `{escape_markdown(error_message, version=2)}`"
                    )
                    if biz_id:
                        final_error_message += f"\n📊 *Partial Business ID:* `{escape_markdown(biz_id, version=2)}`"
                    await send_telegram_message(update, final_error_message, parse_mode='MarkdownV2')
                    logger.error(f"User {user_id}: Business #{current_businesses_count}: All attempts failed. Final error: {error_message}")
        
        if not current_biz_attempt_success:
            await send_telegram_message(
                update,
                f"⚠️ Business \\#{current_businesses_count} *could not be created after multiple retries.*\n\nMoving to the next attempt...",
                parse_mode='MarkdownV2'
            )
            time.sleep(random.randint(10, 20))
        else:
            await send_telegram_message(
                update,
                f"✅ Business \\#{current_businesses_count} created.\n\n⏳ Waiting a bit before next attempt...",
                parse_mode='MarkdownV2'
            )
            time.sleep(random.randint(5, 15))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a help message with buttons."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    message = (
        "🤖 *Bot Help Guide*\n\n"
        "I can help you create *Facebook Business Manager* accounts automatically.\n\n"
        "📌 *Steps to get started:*\n"
        "1️⃣ Send me your *Facebook cookies* as a single line of text.\n"
        "2️⃣ Open the *Control Panel* using the button below.\n"
        "3️⃣ Press *Start New Session* to begin.\n\n"
        "⚠️ Note:\n"
        "- Each business may take a few minutes.\n"
        "- Retries are automatic for better success.\n"
        "- For TempMail API setup, please contact the *Admin*."
    )

    keyboard = [
        [InlineKeyboardButton("📂 Open Control Panel", callback_data="open_panel")],
        [InlineKeyboardButton("📞 Contact Admin", url="https://t.me/YourAdminUsername")]  # غير اليوزر نيم
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await send_telegram_message(
        update,
        message,
        parse_mode="MarkdownV2",
        reply_markup=reply_markup
    )

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin panel command."""
    user_id = update.effective_user.id
    user_data = get_user(user_id)

    if not user_data or not user_data['is_admin']:
        await send_telegram_message(update, "❌ You do not have admin privileges to access this panel\\.", parse_mode='MarkdownV2')
        return

    await send_admin_panel(update, context)

async def send_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE, user_page: int = 0) -> None:
    """Sends the admin control panel."""
    users = get_all_users()
    total_users = len(users)
    users_per_page = 10
    total_pages = (total_users + users_per_page - 1) // users_per_page
    
    start_index = user_page * users_per_page
    end_index = start_index + users_per_page
    users_on_page = users[start_index:end_index]

    # عنوان البانيل
    message_text = f"⚙️ *Admin Control Panel*\n👥 *Total Users:* `{total_users}`\n\n"

    if not users_on_page:
        message_text += "❌ *No users to display.*"
    else:
        for user_info in users_on_page:  # Renamed 'user' to 'user_info' to avoid conflict
            sub_status = "❌ Inactive"
            if user_info['subscription_end_date']:
                if user_info['subscription_end_date'] >= date.today():
                    days_left = (user_info['subscription_end_date'] - date.today()).days
                    sub_status = f"✅ Active ({days_left} days left)"
                else:
                    sub_status = "⚠️ Expired"
            
            admin_status = " 👑 *[Admin]*" if user_info['is_admin'] else ""
            message_text += (
                f"👤 *User ID:* `{user_info['user_id']}`{admin_status}\n"
                f"📅 *Joined:* `{user_info['created_at'].strftime('%Y-%m-%d')}`\n"
                f"✨ *Subscription:* {sub_status}\n"
                f"🏢 *Businesses:* `{user_info['businesses_created_count']}`\n"
                f"⚙️ *Status:* `{user_info['status']}`\n"
                "───────────────\n"
            )
    
    # أزرار التحكم
    keyboard = [
        [InlineKeyboardButton("➕ Activate Subscription", callback_data="admin_activate_sub")],
        [InlineKeyboardButton("🔑 Set TempMail API", callback_data="admin_set_tempmail_api")],
        [InlineKeyboardButton("📊 General Stats", callback_data="admin_stats")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="admin_refresh_panel")]
    ]

    # أزرار التنقل بين الصفحات
    pagination_buttons = []
    if user_page > 0:
        pagination_buttons.append(
            InlineKeyboardButton("⬅️ Previous", callback_data=f"admin_users_page_{user_page - 1}")
        )
    if user_page < total_pages - 1:
        pagination_buttons.append(
            InlineKeyboardButton("Next ➡️", callback_data=f"admin_users_page_{user_page + 1}")
        )
    if pagination_buttons:
        keyboard.append(pagination_buttons)

    await send_telegram_message(
        update,
        message_text,
        parse_mode='MarkdownV2',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def admin_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles admin panel button presses."""
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    user_data = get_user(user_id)

    if not user_data or not user_data['is_admin']:
        await send_telegram_message(update, "❌ You do not have admin privileges\\.", parse_mode='MarkdownV2')
        return

    action = query.data

    if action == "admin_activate_sub":
        await send_telegram_message(update, "To activate subscription, send command in format:\n`/activate_sub <user_id> <days>`\nExample: `/activate_sub 123456789 30` (for 30 days)", parse_mode='MarkdownV2')
    
    elif action == "admin_set_tempmail_api":
        await send_telegram_message(update, "To set TempMail API key for a user, send command in format:\n`/set_user_tempmail_api <user_id> <api_key>`\nExample: `/set_user_tempmail_api 123456789 131|nLYDwolNM8987MZX7s0At3muNsbK7S7tJ3nKrrvu1677734a`", parse_mode='MarkdownV2')

    elif action == "admin_stats":
        total_users = len(get_all_users())
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT SUM(businesses_created_count) FROM users;")
            total_businesses = cur.fetchone()[0] or 0
            
            cur.execute("SELECT COUNT(*) FROM users WHERE subscription_end_date >= CURRENT_DATE;")
            active_subscriptions = cur.fetchone()[0] or 0

            message = (
                f"📊 *General Statistics:*\n"
                f"  Total Users: `{total_users}`\n"
                f"  Active Subscriptions: `{active_subscriptions}`\n"
                f"  Total Businesses Created: `{total_businesses}`"
            )
            await send_telegram_message(update, message, parse_mode='MarkdownV2')
        except Exception as e:
            logger.error(f"Error getting admin stats: {e}")
            await send_telegram_message(update, f"❌ Error fetching statistics: {escape_markdown(str(e), version=2)}", parse_mode='MarkdownV2')
        finally:
            if conn:
                release_db_connection(conn)

    elif action == "admin_refresh_panel":
        await send_admin_panel(update, context)
    
    elif action.startswith("admin_users_page_"):
        page = int(action.split('_')[-1])
        await send_admin_panel(update, context, user_page=page)

async def activate_subscription_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command to activate user subscription."""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_telegram_message(update, "❌ You do not have admin privileges\\.", parse_mode='MarkdownV2')
        return

    if len(context.args) != 2:
        await send_telegram_message(update, "❌ Incorrect usage\\. Correct format: `/activate_sub <user_id> <days>`", parse_mode='MarkdownV2')
        return

    try:
        target_user_id = int(context.args[0])
        days = int(context.args[1])
        
        target_user_data = get_user(target_user_id)
        if not target_user_data:
            # Create user if not exists
            create_or_update_user(target_user_id)
            target_user_data = get_user(target_user_id) # Re-fetch to get default values

        current_end_date = target_user_data['subscription_end_date'] or date.today()
        if current_end_date < date.today():
            current_end_date = date.today() # Start from today if expired

        new_end_date = current_end_date + timedelta(days=days)
        
        create_or_update_user(target_user_id, subscription_end_date=new_end_date)
        await send_telegram_message(update, f"✅ Subscription activated for user `{target_user_id}` until `{new_end_date.strftime('%Y-%m-%d')}`\\.", parse_mode='MarkdownV2')
        # Notify target user
        try:
            await context.bot.send_message(chat_id=target_user_id, text=f"🎉 Your subscription has been activated successfully until `{new_end_date.strftime('%Y-%m-%d')}`\\.", parse_mode='MarkdownV2')
        except Exception as e:
            logger.warning(f"Could not notify user {target_user_id} about subscription activation: {e}")

    except ValueError:
        await send_telegram_message(update, "❌ Invalid user ID or number of days\\. Please enter valid numbers\\.", parse_mode='MarkdownV2')
    except Exception as e:
        logger.error(f"Error activating subscription: {e}")
        await send_telegram_message(update, f"❌ Error activating subscription: {escape_markdown(str(e), version=2)}", parse_mode='MarkdownV2')

async def set_user_tempmail_api_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command to set TempMail API key for a user."""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_telegram_message(update, "❌ You do not have admin privileges\\.", parse_mode='MarkdownV2')
        return

    if len(context.args) != 2:
        await send_telegram_message(update, "❌ Incorrect usage\\. Correct format: `/set_user_tempmail_api <user_id> <api_key>`", parse_mode='MarkdownV2')
        return

    try:
        target_user_id = int(context.args[0])
        api_key = context.args[1]
        
        target_user_data = get_user(target_user_id)
        if not target_user_data:
            # Create user if not exists
            create_or_update_user(target_user_id)
            
        update_user_tempmail_config(target_user_id, api_key=api_key)
        await send_telegram_message(update, f"✅ TempMail API key set for user `{target_user_id}` successfully\\.", parse_mode='MarkdownV2')
        # Notify target user
        try:
            await context.bot.send_message(chat_id=target_user_id, text="🎉 Your TempMail API key has been set successfully\\.", parse_mode='MarkdownV2')
        except Exception as e:
            logger.warning(f"Could not notify user {target_user_id} about API key setting: {e}")

    except ValueError:
        await send_telegram_message(update, "❌ Invalid user ID\\. Please enter a valid number\\.", parse_mode='MarkdownV2')
    except Exception as e:
        logger.error(f"Error setting user TempMail API key: {e}")
        await send_telegram_message(update, f"❌ Error setting user TempMail API key: {escape_markdown(str(e), version=2)}", parse_mode='MarkdownV2')

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
    elif update.callback_query:
        await send_telegram_message(update, message, parse_mode='MarkdownV2')
    else:
        logger.warning("Error handler called without an effective message or callback query.")

def main_telegram_bot():
    """Starts the Telegram bot."""
    logger.info("Starting Telegram Bot...")
    
    # Initialize database schema
    init_db() 

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("admin", admin_command))
    application.add_handler(CommandHandler("activate_sub", activate_subscription_command))
    application.add_handler(CommandHandler("set_user_tempmail_api", set_user_tempmail_api_command))

    # Message handler for cookies (any text message that is not a command)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_cookies_message))

    # Callback query handler for inline buttons
    application.add_handler(CallbackQueryHandler(handle_callback_query, pattern="^(start_creation|pause_creation|resume_creation|download_invitations|download_ids|refresh_panel|show_admin_panel)$"))
    application.add_handler(CallbackQueryHandler(admin_callback_query, pattern="^admin_"))

    # Register error handler
    application.add_error_handler(error_handler) 

    logger.info("Bot is running. Press Ctrl+C to stop.")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main_telegram_bot()
