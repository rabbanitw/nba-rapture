from datetime import datetime

def convert_time(filename):
  #Grab wayback stamp from filename
  waystamp = ''.join(filter(str.isdigit, filename))

  #Wayback time format YYYYMMDDhhmmss
  date_object = datetime.strptime(waystamp, "%Y%m%d%H%M%S")
  convert_date = date_object.strftime("%Y-%m-%d")
  return convert_date
