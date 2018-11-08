import boto3
import datetime
from urllib.request import urlopen
import json
import decimal


def url_builder(city_id):
    user_api = ''  # Obtain yours form: http://openweathermap.org/
    unit = 'metric'  # For Fahrenheit use imperial, for Celsius use metric, and the default is Kelvin.
    api = 'http://api.openweathermap.org/data/2.5/weather?id='  # Search for your city ID here: http://bulk.openweathermap.org/sample/city.list.json.gz
    #api.openweathermap.org / data / 2.5 / forecast?id = 524901
    full_api_url = api + str(city_id) + '&mode=json&units=' + unit + '&APPID=' + user_api
    return full_api_url


def data_fetch(city_id):
    full_api_url = url_builder(city_id)
    url = urlopen(full_api_url)
    output = url.read().decode('utf-8')
    raw_api_dict = json.loads(output, parse_float=decimal.Decimal)
    url.close()
    return raw_api_dict


def lambda_handler(event, context):
    print('Checking...')  # {} at {}...'.format(SITE, event['time']))

    try:
        cityList = [6173331, 6085772, 6111632, 7281936, 5946768, 6077243, 6094817, 5913490]
        for item in cityList:
            sampledata = data_fetch(item)

            userTable = boto3.resource('dynamodb').Table('CurrentWeather')

            cityId = int(sampledata['id'])
            cityName = str(sampledata['name'])
            wdt = int(sampledata['dt'])
            info = sampledata
            userTable.put_item(
                Item={
                    'CityId': cityId,
                    'UTCDateTime': wdt,
                    'CityName': cityName,
                    'Info': info,
                }
            )

    except:
        print('Check failed!')
        raise
    else:
        print('Check passed!')
        return event['time']
    finally:
        print('Data successfully stored in DynamoDB')

