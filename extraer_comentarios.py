import pandas as pd
from apify_client import ApifyClient
import time
import re
import logging
import html
import unicodedata
import os

# Configurar logging más limpio
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# --- PARÁMETROS DE CONFIGURACIÓN ---
APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
SOLO_PRIMER_POST = False

# LISTA DE URLs A PROCESAR
URLS_A_PROCESAR = [
    # Instagram Posts
    "https://www.instagram.com/p/DM_Fcv3A_Ym/#advertiser",
    "https://www.instagram.com/p/DM_FWE8AdHo/#advertiser",
    "https://www.instagram.com/p/DM8rSlNAWwb/#advertiser",
    "https://www.instagram.com/p/DM8rSMrA8KP/#advertiser",
    "https://www.instagram.com/p/DM8rRHrAIoH/#advertiser",
    "https://www.instagram.com/p/DM8rTEEAJjq/#advertiser",
    "https://www.instagram.com/p/DM_Frbkg7v4/#advertiser",
    "https://www.instagram.com/p/DM-YTM8gJAI/#advertiser",
    "https://www.instagram.com/p/DM-YT9qgx_s/#advertiser",
    "https://www.instagram.com/p/DM-YS4tgsrV/#advertiser",
    "https://www.instagram.com/p/DM-YQafgQOP/#advertiser",
    "https://www.instagram.com/p/DM-YSq3g2Uj/#advertiser",
    "https://www.instagram.com/p/DNZPfjugCDo/#advertiser",
    "https://www.instagram.com/p/DNZQK6ngS8u/#advertiser",
    "https://www.instagram.com/p/DNVr1GRA2Sr/#advertiser",
    "https://www.instagram.com/p/DNVrxfVAV3l/#advertiser",
    "https://www.instagram.com/p/DNS-Zw-A6Oi/#advertiser",
    "https://www.instagram.com/p/DNVr0veg684/#advertiser",
    "https://www.instagram.com/p/DNnjkRRAoki/#advertiser",
    "https://www.instagram.com/p/DNnjkPbAf58/#advertiser",
    "https://www.instagram.com/p/DNnw86mgBd8/#advertiser",
    "https://www.instagram.com/p/DNnwsoQgcVF/#advertiser",
    
    # Facebook Posts
    "https://www.facebook.com/100064672685926/posts/1170398145125931/?dco_ad_token=Aaqd93VZDxxTsTiHXggZLy7LWvlNV1DbZBaTFkbBPh8dNg78qhVQk-zZbrlhhupVq66k9m5YgC1Fmk4P&dco_ad_id=120228919741640701",
    "https://www.facebook.com/100064672685926/posts/1163143839184695/?dco_ad_token=Aap7vHiGm82HWrjGhWmc8xAw_J98sN3OiU59j_wFvJPSDxN4vEgdcEw-y-WCEdArHenBq3CWZUNQym01&dco_ad_id=120232299228210295",
    "https://www.facebook.com/100064672685926/posts/1162323169266762/?dco_ad_token=AaooMUXLR-e7-QfJxKoWQVNr-NEoidsMF3fWCIK4u1kxp8cnDwO6GyZAssGPanBN1ptFiTvun_1u4nYf&dco_ad_id=120232299228190295",
    "https://www.facebook.com/100064672685926/posts/1162323155933430/?dco_ad_token=AarFLLT6wgqioJr-olbFQeRcO2Q8Zktd6YoSVAkKtMDIyh1_4MRUcN1Lpaxrv1JvGcqKoEN47K--HUGF&dco_ad_id=120232305026440295",
    "https://www.facebook.com/100064672685926/posts/1162323182600094/?dco_ad_token=AaptpycfjM4aUVn-pw9gqfqy_HQ4jXu6b7g9VqOROpG1nmrCwxlzo_5LfkSJPbvvJpdWHdkKm3vINzhS&dco_ad_id=120232299228200295",
    "https://www.facebook.com/100064672685926/posts/1165390675626678/?dco_ad_token=Aaqk16ep0aU0pTquD5AwKeadseCrih99vLQ8NQLGU7wp5LUSnfk-9N2y2Hs7dypiYeLMTjEQtzBJg7R8&dco_ad_id=120232340398560295",
    "https://www.facebook.com/100064672685926/posts/1165390352293377/?dco_ad_token=AaojLUFFmxTBPr0FeMuCtnJMldsgOsd2nJNsvZ6gE5WcYMPIAk4nKsfMKi7W7OOnU1UmM-FFIu-w4uJK&dco_ad_id=120232026445930295",
    "https://www.facebook.com/100064672685926/posts/1162872455878500/?dco_ad_token=AaqHd-M5Z9X7mJei5WVnOO6EeIo0K1566IiVHD8DnXEWN0D8tjV4BWXpNuOOcacjrSH3i_mGQOuAkQ4j&dco_ad_id=120232340087710295",
    "https://www.facebook.com/100064672685926/posts/1162872185878527/?dco_ad_token=AaqcfB_lNbSv06o8CaeQ2thlcQJTm-Nvrwgme5TSKMg4e7v0ttQ5QDTzWIqN2AEGLYN9SEIblWMqKGMx&dco_ad_id=120232028234180295",
    "https://www.facebook.com/100064672685926/posts/1162871792545233/?dco_ad_token=AaozV8DPHej7DzQmz2qqi8wOJFFbd00_sMR75miLv05dtRnbYjgRw2q66pmBDuoZTJ_W1KVcNcr1q42Z&dco_ad_id=120232340527270295",
    "https://www.facebook.com/100064672685926/posts/1162872155878530/?dco_ad_token=AaonqmmC4FdIqX-BskkG9VQYUf0aamZ9BCgMtLbw83Ae4qja5rMbnihIc6b4TDXe6PmoorHjWrT0RDnE&dco_ad_id=120232340323260295",
    "https://www.facebook.com/100064672685926/posts/1171691858329893/?dco_ad_token=Aao-Qa2-I3onn6xHjClcg5lcLjb4MutTawoHgHfFWcYN5CIWnYdXouY2sJANMfRhT55QK_xNZSwWo0TR&dco_ad_id=120232859947670295",
    "https://www.facebook.com/100064672685926/posts/1170398088459270/?dco_ad_token=Aar7jiJ_QDvM3adKRumJb_DTX9ri915oAcGpB0aNjtf_7txo63YK6tO7ZBgkHLXCcKDsvnYJAnnWThb6&dco_ad_id=120228919741650701",
    "https://www.facebook.com/100064672685926/posts/1170397798459299/?dco_ad_token=AapMELDnv6xy-OzjDJfPGgygcgxeRnqCgB3OmbAyyqnAys0uuFpsvuW3Or1I8E3THSlgGBciU3XxyQrq&dco_ad_id=120228919741610701",
    "https://www.facebook.com/100064672685926/posts/1170398138459265/?dco_ad_token=Aap-TT7j1RpWUx9VGMjArmBwiefD5ixIY37EeREl0Kl15jEE1U0hLenBUMsyxumYitNK4srGyJMHHUPI&dco_ad_id=120228919741620701",
    "https://www.facebook.com/100064672685926/posts/1170398131792599/?dco_ad_token=AaoFFY4LRKJ8c2E0_-y86Ys7t1matG81R9ZcDdWDW0rs7AVv0zn97dr83esX_ibbfbXaQPwUjGp-lGL1&dco_ad_id=120228919741660701",
    "https://www.facebook.com/100064672685926/posts/1176249481207464/?dco_ad_token=AarfXe-MPJ8xuo5SKEHsPv1dLTr0FTVgo1QF6cvmEqSD--YxzTz3UKtNDr1zbjeRjTJe_YzxUgeblfMk&dco_ad_id=120233151708140295",
    "https://www.facebook.com/100064672685926/posts/1176249024540843/?dco_ad_token=AaqTLFyMFZhtPlS8-B1B7583fH6wU-cPy2VU87qFVvJKzI8UUl1mCmMPF5UwZWYLTxMQzbEXK-5Z4SB3&dco_ad_id=120233112602190295",
    "https://www.facebook.com/100064672685926/posts/1176337131198699/?dco_ad_token=AaqMggo9she_oXbcU53r4SsXG3jH_6kQpXbuBX6XTs5dn-l7V-985swJcUxTM6MrdVnHBdSCvJBfvXnD&dco_ad_id=120233154785120295",
    "https://www.facebook.com/100064672685926/posts/1176336844532061/?dco_ad_token=Aaruc96lxzeZ9Rec22yuREJkxJBjWHkhwVpsSvBAXTckhueaQwYzQKUMpnDDT3tu2FC_sBC1lqi20RHa&dco_ad_id=120233155761890295",
    
    # TikTok Videos
    "https://www.tiktok.com/@alpinacol/video/7545531175264144658?_r=1&_t=ZS-8zmBxpXxaMB",
    "https://www.tiktok.com/@alpinacol/video/7545536081081715986?_r=1&_t=ZS-8zmBohiJNWk",
    "https://www.tiktok.com/@alpinacol/video/7545537780177227015?_r=1&_t=ZS-8zmBj8nzZVS",
    "https://www.tiktok.com/@alpinacol/video/7545543093358038273?_r=1&_t=ZS-8zmBflgSzMV",
    "https://www.tiktok.com/@alpinacol/video/7545543148685004039?_r=1&_t=ZS-8zmBbHs0rLe",
    "https://www.tiktok.com/@alpinacol/video/7545589068067851536?_r=1&_t=ZS-8zmBUxpzRuZ"
]

# INFORMACIÓN DE CAMPAÑA
CAMPAIGN_INFO = {
    'campaign_name': 'CAMPAÑA_MANUAL_MULTIPLE',
    'campaign_id': 'MANUAL_002',
    'campaign_mes': 'Septiembre 2025',
    'campaign_marca': 'TU_MARCA',
    'campaign_referencia': 'REF_MANUAL',
    'campaign_objetivo': 'Análisis de Comentarios'
}

class SocialMediaScraper:
    def __init__(self, apify_token):
        self.client = ApifyClient(apify_token)

    def detect_platform(self, url):
        if pd.isna(url) or not url: return None
        url = str(url).lower()
        if any(d in url for d in ['facebook.com', 'fb.com']): return 'facebook'
        if 'instagram.com' in url: return 'instagram'
        if 'tiktok.com' in url: return 'tiktok'
        return None

    def clean_url(self, url):
        return str(url).split('?')[0] if '?' in str(url) else str(url)

    def fix_encoding(self, text):
        if pd.isna(text) or text == '': return ''
        try:
            text = str(text)
            text = html.unescape(text)
            text = unicodedata.normalize('NFKD', text)
            return text.strip()
        except Exception as e:
            logger.warning(f"Could not fix encoding: {e}")
            return str(text)

    def _wait_for_run_finish(self, run):
        logger.info("Scraper initiated, waiting for results...")
        max_wait_time = 300
        start_time = time.time()
        while True:
            run_status = self.client.run(run["id"]).get()
            if run_status["status"] in ["SUCCEEDED", "FAILED", "TIMED-OUT"]:
                return run_status
            if time.time() - start_time > max_wait_time:
                logger.error("Timeout reached while waiting for scraper.")
                return None
            time.sleep(10)

    def scrape_facebook_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing Facebook Post {post_number}: {url}")
            run_input = {"startUrls": [{"url": self.clean_url(url)}], "maxComments": max_comments}
            run = self.client.actor("apify/facebook-comments-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"Facebook extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} items found.")
            return self._process_facebook_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Fatal error in scrape_facebook_comments: {e}")
            return []

    def scrape_instagram_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing Instagram Post {post_number}: {url}")
            run_input = {"directUrls": [url], "resultsType": "comments", "resultsLimit": max_comments}
            run = self.client.actor("apify/instagram-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"Instagram extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} items found.")
            return self._process_instagram_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Fatal error in scrape_instagram_comments: {e}")
            return []

    def scrape_tiktok_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing TikTok Post {post_number}: {url}")
            run_input = {"postURLs": [self.clean_url(url)], "maxCommentsPerPost": max_comments}
            run = self.client.actor("clockworks/tiktok-comments-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"TikTok extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} comments found.")
            return self._process_tiktok_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Fatal error in scrape_tiktok_comments: {e}")
            return []

    def _process_facebook_results(self, items, url, post_number, campaign_info):
        processed = []
        # <-- CORRECCIÓN: Usando tu lista de campos de fecha más completa
        possible_date_fields = ['createdTime', 'timestamp', 'publishedTime', 'date', 'createdAt', 'publishedAt']
        for comment in items:
            # <-- CORRECCIÓN: Usando tu bucle for original para máxima compatibilidad
            created_time = None
            for field in possible_date_fields:
                if field in comment and comment[field]:
                    created_time = comment[field]
                    break
            comment_data = {**campaign_info, 'post_url': url, 'post_number': post_number, 'platform': 'Facebook', 'author_name': self.fix_encoding(comment.get('authorName')), 'author_url': comment.get('authorUrl'), 'comment_text': self.fix_encoding(comment.get('text')), 'created_time': created_time, 'likes_count': comment.get('likesCount', 0), 'replies_count': comment.get('repliesCount', 0), 'is_reply': False, 'parent_comment_id': None, 'created_time_raw': str(comment)}
            processed.append(comment_data)
        logger.info(f"Processed {len(processed)} Facebook comments.")
        return processed

    def _process_instagram_results(self, items, url, post_number, campaign_info):
        processed = []
        # <-- CORRECCIÓN: Usando tu lista de campos de fecha más completa
        possible_date_fields = ['timestamp', 'createdTime', 'publishedAt', 'date', 'createdAt', 'taken_at']
        for item in items:
            comments_list = item.get('comments', [item]) if item.get('comments') is not None else [item]
            for comment in comments_list:
                # <-- CORRECCIÓN: Usando tu bucle for original
                created_time = None
                for field in possible_date_fields:
                    if field in comment and comment[field]:
                        created_time = comment[field]
                        break
                author = comment.get('ownerUsername', '')
                comment_data = {**campaign_info, 'post_url': url, 'post_number': post_number, 'platform': 'Instagram', 'author_name': self.fix_encoding(author), 'author_url': f"https://instagram.com/{author}", 'comment_text': self.fix_encoding(comment.get('text')), 'created_time': created_time, 'likes_count': comment.get('likesCount', 0), 'replies_count': 0, 'is_reply': False, 'parent_comment_id': None, 'created_time_raw': str(comment)}
                processed.append(comment_data)
        logger.info(f"Processed {len(processed)} Instagram comments.")
        return processed

    def _process_tiktok_results(self, items, url, post_number, campaign_info):
        processed = []
        for comment in items:
            author_id = comment.get('user', {}).get('uniqueId', '')
            comment_data = {**campaign_info, 'post_url': url, 'post_number': post_number, 'platform': 'TikTok', 'author_name': self.fix_encoding(comment.get('user', {}).get('nickname')), 'author_url': f"https://www.tiktok.com/@{author_id}", 'comment_text': self.fix_encoding(comment.get('text')), 'created_time': comment.get('createTime'), 'likes_count': comment.get('diggCount', 0), 'replies_count': comment.get('replyCommentTotal', 0), 'is_reply': 'replyToId' in comment, 'parent_comment_id': comment.get('replyToId'), 'created_time_raw': str(comment)}
            processed.append(comment_data)
        logger.info(f"Processed {len(processed)} TikTok comments.")
        return processed

def save_to_excel(df, filename):
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Comentarios', index=False)
            if 'post_number' in df.columns:
                summary = df.groupby(['post_number', 'platform', 'post_url']).agg(Total_Comentarios=('comment_text', 'count'), Total_Likes=('likes_count', 'sum')).reset_index()
                summary.to_excel(writer, sheet_name='Resumen_Posts', index=False)
        logger.info(f"Excel file saved successfully: {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving Excel file: {e}")
        return False

def process_datetime_columns(df):
    if 'created_time' not in df.columns: return df
    logger.info("Processing datetime columns...")
    # Intenta convertir todo tipo de formatos (timestamps, ISO, etc.) a un datetime unificado
    df['created_time_processed'] = pd.to_datetime(df['created_time'], errors='coerce', utc=True, unit='s')
    mask = df['created_time_processed'].isna()
    df.loc[mask, 'created_time_processed'] = pd.to_datetime(df.loc[mask, 'created_time'], errors='coerce', utc=True)
    if df['created_time_processed'].notna().any():
        df['created_time_processed'] = df['created_time_processed'].dt.tz_localize(None)
        df['fecha_comentario'] = df['created_time_processed'].dt.date
        df['hora_comentario'] = df['created_time_processed'].dt.time
    return df

def run_extraction():
    logger.info("--- STARTING COMMENT EXTRACTION PROCESS ---")
    if not APIFY_TOKEN:
        logger.error("APIFY_TOKEN not found in environment variables. Aborting.")
        return

    valid_urls = [url.strip() for url in URLS_A_PROCESAR if url.strip()]
    if not valid_urls:
        logger.warning("No valid URLs to process. Exiting.")
        return

    scraper = SocialMediaScraper(APIFY_TOKEN)
    all_comments = []
    post_counter = 0

    for url in valid_urls:
        post_counter += 1
        platform = scraper.detect_platform(url)
        comments = []
        if platform == 'facebook':
            comments = scraper.scrape_facebook_comments(url, campaign_info=CAMPAIGN_INFO, post_number=post_counter)
        elif platform == 'instagram':
            comments = scraper.scrape_instagram_comments(url, campaign_info=CAMPAIGN_INFO, post_number=post_counter)
        elif platform == 'tiktok':
            comments = scraper.scrape_tiktok_comments(url, campaign_info=CAMPAIGN_INFO, post_number=post_counter)
        else:
            logger.warning(f"Unknown platform for URL: {url}")
        
        all_comments.extend(comments)
        if not SOLO_PRIMER_POST and post_counter < len(valid_urls):
            logger.info("Pausing for 30 seconds between posts...")
            time.sleep(30)

    if not all_comments:
        logger.warning("No comments were extracted. Process finished.")
        return

    logger.info("--- PROCESSING FINAL RESULTS ---")
    df_comments = pd.DataFrame(all_comments)
    df_comments = process_datetime_columns(df_comments)
    
    final_columns = ['post_number', 'platform', 'campaign_name', 'post_url', 'author_name', 'comment_text', 'created_time_processed', 'fecha_comentario', 'hora_comentario', 'likes_count', 'replies_count', 'is_reply', 'author_url', 'created_time_raw']
    existing_cols = [col for col in final_columns if col in df_comments.columns]
    df_comments = df_comments[existing_cols]

    filename = "Comentarios Campaña.xlsx"
    save_to_excel(df_comments, filename)
    logger.info("--- EXTRACTION PROCESS FINISHED ---")

if __name__ == "__main__":
    run_extraction()








