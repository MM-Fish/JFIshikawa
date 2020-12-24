from scrape_modules import ScrapeIshikawa
import requests, json
import slackweb
json_open = open('important.json', 'r')
important_list = json.load(json_open)

# データスクレイピング
sps_url = important_list['sps_url']
si = ScrapeIshikawa(sps_url)
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