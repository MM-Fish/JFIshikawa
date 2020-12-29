from scrape_modules import ScrapeIshikawa
import requests
import slackweb
import os
import json

google_api_json={
  "type": os.environ["type"].replace('\\n', '\n'),
  "project_id": os.environ["project_id"].replace('\\n', '\n'),
  "private_key_id": os.environ["private_key_id"].replace('\\n', '\n'),
  "private_key": os.environ["private_key"].replace('\\n', '\n'),
  "client_email": os.environ["client_email"].replace('\\n', '\n'),
  "client_id": os.environ["client_id"].replace('\\n', '\n'),
  "auth_uri": os.environ["auth_uri"].replace('\\n', '\n'),
  "token_uri": os.environ["token_uri"].replace('\\n', '\n'),
  "auth_provider_x509_cert_url": os.environ["auth_provider_x509_cert_url"].replace('\\n', '\n'),
  "client_x509_cert_url": os.environ["client_x509_cert_url"].replace('\\n', '\n')
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

# 新しい市況が取得できた場合のみslackに通知
print(si.sps_data)
if len(si.sps_data) > 0:
  content = {
      "icon_url" : "https://drive.google.com/uc?export=view&id=1bbPewWm7dHriVWqHQ2IJe3uzwbN1-RHE",
      'username': "JF石川市況",
      "text": f"JF石川の市況データを更新しました\n{os.environ['sps_url']}"
      }
  webhook_url = os.environ["slack_webhook_url"]
  requests.post(webhook_url, data = json.dumps(content))