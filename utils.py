from datetime import datetime
import re

'''
### NBA Regular Season and Postseason Dates (2013–2023)

| Season  | Regular Season Start | Regular Season End | Postseason Start | Postseason End |
|---------|-----------------------|--------------------|------------------|----------------|
| 2013-14 | 2013-10-29           | 2014-04-16        | 2014-04-19       | 2014-06-15     |
| 2014-15 | 2014-10-28           | 2015-04-15        | 2015-04-18       | 2015-06-16     |
| 2015-16 | 2015-10-27           | 2016-04-13        | 2016-04-16       | 2016-06-19     |
| 2016-17 | 2016-10-25           | 2017-04-12        | 2017-04-15       | 2017-06-12     |
| 2017-18 | 2017-10-17           | 2018-04-11        | 2018-04-14       | 2018-06-08     |
| 2018-19 | 2018-10-16           | 2019-04-10        | 2019-04-13       | 2019-06-13     |
| 2019-20 | 2019-10-22           | 2020-03-11        | 2020-08-17       | 2020-10-11     |
| 2020-21 | 2020-12-22           | 2021-05-16        | 2021-05-22       | 2021-07-20     |
| 2021-22 | 2021-10-19           | 2022-04-10        | 2022-04-16       | 2022-06-16     |
| 2022-23 | 2022-10-18           | 2023-04-09        | 2023-04-15       | 2023-06-12     |
'''


def get_season(waystamp):
  if waystamp >= wayback_time("2013-10-29") and waystamp < wayback_time("2014-10-28"):
    return '2013-14'
  elif waystamp >= wayback_time("2014-10-28") and waystamp < wayback_time("2015-10-27"):
    return '2014-15'
  elif waystamp >= wayback_time("2015-10-27") and waystamp < wayback_time("2016-10-25"):
    return '2015-16'
  elif waystamp >= wayback_time("2016-10-25") and waystamp < wayback_time("2017-10-17"):
    return '2016-17'
  elif waystamp >= wayback_time("2017-10-17") and waystamp < wayback_time("2018-10-16"):
    return '2017-18'
  elif waystamp >= wayback_time("2018-10-16") and waystamp < wayback_time("2019-10-22"):
    return '2018-19'
  elif waystamp >= wayback_time("2019-10-22") and waystamp < wayback_time("2020-12-22"):
    return '2019-20'
  elif waystamp >= wayback_time("2020-12-22") and waystamp < wayback_time("2021-10-19"):
    return '2020-21'
  elif waystamp >= wayback_time("2021-10-19") and waystamp < wayback_time("2022-10-18"):
    return '2021-22'
  elif waystamp >= wayback_time("2022-10-18") and waystamp < wayback_time("2023-10-24"):
    return '2022-23'
  elif waystamp >= wayback_time("2023-10-24") and waystamp < wayback_time("2024-10-22"):
    return '2023-24'
  elif waystamp >= wayback_time("2024-10-22") and waystamp < wayback_time("2025-10-21"):
    return '2024-25'
  else:
    print(f"Error: timestamp={waystamp}")
    raise Exception(f"Error: timestamp={waystamp}")

def inside_range(timestamp, end):
  return timestamp < wayback_time(end)


def get_date_range(timestamp, season_type):

  season = get_season(timestamp)

  match season:
    case '2020-21':
      if season_type == "Playoffs":
        if inside_range(timestamp,'2021-07-20'):
          return ['2021-05-22',regular_time(timestamp)]
        else:
          return ""
      elif season_type == "Regular season":
        if inside_range(timestamp,'2021-05-22'):
          return ['2020-12-22',regular_time(timestamp)]
      else:
        return ['2020-12-22',regular_time(timestamp)]
    case '2021-22':
      if season_type == "Playoffs":
        if inside_range(timestamp,'2022-06-16'):
          return ['2022-04-16',regular_time(timestamp)]
        else:
          return ""
      elif season_type == "Regular season":
        if inside_range(timestamp, "2022-04-16"):
          return ["2021-10-19",regular_time(timestamp)]
      else:
        return ["2021-10-19",regular_time(timestamp)]
    case '2022-23':
      if season_type == "Playoffs":
        if inside_range(timestamp, "2023-06-12"):
          return ["2023-04-15",regular_time(timestamp)]
        else:
          return ""
      elif season_type == "Regular season":
        if inside_range(timestamp,"2023-04-15"):
          return ['2022-10-18', regular_time(timestamp)]
      else:
          return ['2022-10-18', regular_time(timestamp)]

## Note - use this instead of get_date_range()
def get_date_range_extended(timestamp, season_type):
  season = get_season(timestamp)

  # 2024-25 season
  if season == '2024-25':
    if season_type == "Playoffs":
      if inside_range(timestamp, '2025-06-19'):  # Estimated Finals end
        return ['2025-04-19', regular_time(timestamp)]
    elif season_type == "Regular season":
      if inside_range(timestamp, '2025-04-19'):
        return ['2024-10-22', regular_time(timestamp)]
    else:
      return ['2024-10-22', regular_time(timestamp)]

  # 2023-24 season
  elif season == '2023-24':
    if season_type == "Playoffs":
      if inside_range(timestamp, '2024-06-17'):
        return ['2024-04-20', regular_time(timestamp)]
    elif season_type == "Regular season":
      if inside_range(timestamp, '2024-04-20'):
        return ['2023-10-24', regular_time(timestamp)]
    else:
      return ['2023-10-24', regular_time(timestamp)]

  # 2022-23 season
  elif season == '2022-23':
    if season_type == "Playoffs":
      if inside_range(timestamp, '2023-06-12'):
        return ['2023-04-15', regular_time(timestamp)]
    elif season_type == "Regular season":
      if inside_range(timestamp, '2023-04-15'):
        return ['2022-10-18', regular_time(timestamp)]
    else:
      return ['2022-10-18', regular_time(timestamp)]

  # 2021-22 season
  elif season == '2021-22':
    if season_type == "Playoffs":
      if inside_range(timestamp, '2022-06-16'):
        return ['2022-04-16', regular_time(timestamp)]
    elif season_type == "Regular season":
      if inside_range(timestamp, '2022-04-16'):
        return ['2021-10-19', regular_time(timestamp)]
    else:
      return ['2021-10-19', regular_time(timestamp)]

  # 2020-21 season (COVID-affected)
  elif season == '2020-21':
    if season_type == "Playoffs":
      if inside_range(timestamp, '2021-07-20'):
        return ['2021-05-22', regular_time(timestamp)]
    elif season_type == "Regular Season":
      if inside_range(timestamp, '2021-05-22'):
        return ['2020-12-22', regular_time(timestamp)]
    else:
      return ['2020-12-22', regular_time(timestamp)]

  # 2019-20 season (COVID-affected)
  elif season == '2019-20':
    if season_type == "Playoffs":
      return ['2020-08-17', '2020-10-11']  # Playoffs resumed in bubble
    elif season_type == "Regular Season":
      return ['2019-10-22', '2020-08-17']
    else:
      return ['2019-10-22', '2020-10-11']

  # 2018-19 season
  elif season == '2018-19':
    if season_type == "Playoffs":
      return ['2019-04-13', '2019-06-13']
    elif season_type == "Regular Season":
      return ['2018-10-16', '2019-04-13']
    else:
      return ['2018-10-16', '2019-06-13']

  # 2017-18 season
  elif season == '2017-18':
    if season_type == "Playoffs":
      return ['2018-04-14', '2018-06-08']
    elif season_type == "Regular Season":
      return ['2017-10-17', '2018-04-14']
    else:
      return ['2017-10-17', '2018-06-08']

  # 2016-17 season
  elif season == '2016-17':
    if season_type == "Playoffs":
      return ['2017-04-15', '2017-06-12']
    elif season_type == "Regular Season":
      return ['2016-10-25', '2017-04-15']
    else:
      return ['2016-10-25', '2017-06-12']

  # 2015-16 season
  elif season == '2015-16':
    if season_type == "Playoffs":
      return ['2016-04-16', '2016-06-19']
    elif season_type == "Regular Season":
      return ['2015-10-27', '2016-04-16']
    else:
      return ['2015-10-27', '2016-06-19']

  # 2014-15 season
  elif season == '2014-15':
    if season_type == "Playoffs":
      return ['2015-04-18', '2015-06-16']
    elif season_type == "Regular Season":
      return ['2014-10-28', '2015-04-18']
    else:
      return ['2014-10-28', '2015-06-16']

  # 2013-14 season
  elif season == '2013-14':
    if season_type == "Playoffs":
      return ['2014-04-19', '2014-06-15']
    elif season_type == "Regular Season":
      return ['2013-10-29', '2014-04-19']
    else:
      return ['2013-10-29', '2014-06-15']

  # No matching branch
  raise ValueError(
    f"No date-range rule for season={season} season_type={season_type}"
  )

def regular_time(waystamp):

  #Wayback time format YYYYMMDDhhmmss
  date_object = datetime.strptime(waystamp, "%Y%m%d%H%M%S")
  convert_date = date_object.strftime("%Y-%m-%d")
  return convert_date

def wayback_time(date):

  #PBP date format
  date_object = datetime.strptime(date, "%Y-%m-%d")

  #Turn into wayback timestamp
  convert_date = date_object.strftime("%Y%m%d%H%M%S")

  return convert_date


def remove_numbers_and_apostrophes(string: str) -> str:
  return re.sub(r'[\d\'\-.]+', '', string)


def reformat_date(timestamp):
  date_object = datetime.strptime(timestamp, "%Y-%m-%d")
  return date_object.strftime("%m/%d/%Y")