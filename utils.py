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
  elif waystamp >= wayback_time("2019-10-22") and waystamp < wayback_time("2020-12-22"):
    return '2019-20'
  elif waystamp >= wayback_time("2020-12-22") and waystamp < wayback_time("2021-10-19"):
    return '2020-21'
  elif waystamp >= wayback_time("2021-10-19") and waystamp < wayback_time("2022-10-18"):
    return '2021-22'
  elif waystamp >= wayback_time("2022-10-18") and waystamp <= wayback_time("2023-06-12"):
    return '2022-23'

def inside_range(timestamp, end):
  return timestamp < wayback_time(end)

def get_date_range(timestamp, season_type):

  season = get_season(timestamp)

  match season:
    case '2020-21':
      if season_type == "Playoffs":
        if inside_range(timestamp,'2021-07-20'):
          return ['2021-05-22',regular_time(timestamp)]
      elif season_type == "Regular season":
        if inside_range(timestamp,'2021-05-22'):
          return ['2020-12-22',regular_time(timestamp)]
      else:
        return ['2020-12-22',regular_time(timestamp)]
    case '2021-22':
      if season_type == "Playoffs":
        if inside_range(timestamp,'2022-06-16'):
          return ['2022-04-16',regular_time(timestamp)]
      elif season_type == "Regular season":
        if inside_range(timestamp, "2022-04-16"):
          return ["2021-10-19",regular_time(timestamp)]
      else:
        return ["2021-10-19",regular_time(timestamp)]
    case '2022-23':
      if season_type == "Playoffs":
        if inside_range(timestamp, "2023-06-12"):
          return ["2023-04-15",regular_time(timestamp)]
      elif season_type == "Regular season":
        if inside_range(timestamp,"2023-04-15"):
          return ['2022-10-18', regular_time(timestamp)]
      else:
          return ['2022-10-18', regular_time(timestamp)]

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