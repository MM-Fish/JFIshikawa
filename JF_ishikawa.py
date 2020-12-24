from scrape_modules import ScrapeIshikawa
import requests
import slackweb
import os

google_api_json={
  "type": os.environ["type"],
  "project_id": os.environ["project_id"],
  "private_key_id": os.environ["private_key_id"],
  "private_key": os.environ["private_key"],
  "client_email": os.environ["client_email"],
  "client_id": os.environ["client_id"],
  "auth_uri": os.environ["auth_uri"],
  "token_uri": os.environ["token_uri"],
  "auth_provider_x509_cert_url": os.environ["auth_provider_x509_cert_url"],
  "client_x509_cert_url": os.environ["client_x509_cert_url"]
}


# データスクレイピング
sps_url = os.environ["sps_url"]
si = ScrapeIshikawa(google_api_json, sps_url)
driver = si.open_driver()
# driver = si.open_driver(False)
sikyou_url_list = si.scrape_sikyou_url(driver)
sikyou_url_list_new = si.get_sikyou_url_new(sikyou_url_list)
si.scrape_sikyou_all(driver, sikyou_url_list_new)
# si.scrape_sikyou_all(driver, sikyou_url_list)
driver.quit()

# 魚種列，目方列追加
df_species = si.scrape_data['銘　柄'].map(lambda x: si.container2species_size(x)[0])
df_size = si.scrape_data['銘　柄'].map(lambda x: si.container2species_size(x)[1])
si.scrape_data.insert(3, '魚種',df_species)
si.scrape_data.insert(4, '目方',df_size)

# データ結合
si.concat_new_data()

# データ保存
si.save_sps('市況')

# 魚種に対して全サイズの平均価格を算出。
# 魚種を列，日付を行としたデータフレームを作成。
df_per_day = si.merge_per_day()
df_per_day.head(5)

# データ保存
si.set_with_df("by_species", df_per_day)

# 魚種に対して全サイズの平均価格を算出し，データフレームに格納
df_per_day_and_species = si.merge_all_per_ds()
df_per_day_and_species.head(5)

# データ保存
si.set_with_df("by_species_size", df_per_day_and_species)