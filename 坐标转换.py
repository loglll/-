# /**
#  * @原算法 https://www.jianshu.com/p/57ca061f3987
#  * @根据该作者的修改成JS版：https://blog.csdn.net/qq_28462305/article/details/96277357
#  * @再转 python
#  * @time 2020年2月10日11:22:14
#  * @description bd09 转WGS84,精准度高
#  * */
import math
import pandas as pd

x_pi = float(3.14159265358979324 * 3000.0 / 180.0)
# //pai
pi = float(3.1415926535897932384626)
# //离心率
ee = float(0.00669342162296594323)
# //长半轴
a = float(6378245.0)


# //百度转国测局
def bd09togcj02(bd_lon, bd_lat):
    x = (bd_lon - 0.0065)
    y = (bd_lat - 0.006)
    z = (math.sqrt(x * x + y * y)) - (0.00002 * math.sin(y * x_pi))
    theta = math.atan2(y, x) - 0.000003 * math.cos(x * x_pi)
    gg_lng = z * math.cos(theta)
    gg_lat = z * math.sin(theta)

    return gg_lng, gg_lat


# //国测局转百度
def gcj02tobd09(lng, lat):
    z = math.sqrt(lng * lng + lat * lat) + 0.00002 * math.sin(lat * x_pi)
    theta = math.atan2(lat, lng) + 0.000003 * math.cos(lng * x_pi)
    bd_lng = z * math.cos(theta) + 0.0065
    bd_lat = z * math.sin(theta) + 0.006
    return bd_lng, bd_lat


# //国测局转84
def gcj02towgs84(lng, lat):
    dlat = transformlat(lng - 105.0, lat - 35.0)
    dlng = transformlng(lng - 105.0, lat - 35.0)
    radlat = lat / 180.0 * pi
    magic = math.sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi)
    mglat = lat + dlat
    mglng = lng + dlng
    return lng * 2 - mglng, lat * 2 - mglat


# //经度转换
def transformlat(lng, lat):
    ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + 0.1 * lng * lat + 0.2 * math.sqrt(abs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 * math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lat * pi) + 40.0 * math.sin(lat / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(lat / 12.0 * pi) + 320 * math.sin(lat * pi / 30.0)) * 2.0 / 3.0
    return ret


# //纬度转换
def transformlng(lng, lat):
    ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + 0.1 * lng * lat + 0.1 * math.sqrt(abs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 * math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lng * pi) + 40.0 * math.sin(lng / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(lng / 12.0 * pi) + 300.0 * math.sin(lng / 30.0 * pi)) * 2.0 / 3.0
    return ret


def getWgs84xy(x, y):
    # //先转 国测局坐标
    doubles_gcj = bd09togcj02(x, y)
    # //（x 117.   y 36. ）
    # //国测局坐标转wgs84
    doubles_wgs84 = gcj02towgs84(doubles_gcj[0], doubles_gcj[1])
    # //返回 纠偏后 坐标
    return doubles_wgs84


if __name__ == '__main__':
    df = pd.read_csv('house_info_cleaned_finally.csv')
    df['lon_and_lat_wgs84'] = df.apply(lambda x: getWgs84xy(x['longitude'],x['latitude']),axis=1)
    df['longitude_wgs84'] = df['lon_and_lat_wgs84'].apply(lambda x:x[0])
    df['latitude_wgs84'] = df['lon_and_lat_wgs84'].apply(lambda x: x[1])
    df['is_near_subway'] = df['is_near_subway'].apply(lambda x: '是' if x == '否' else '否')
    df.to_csv('house_info_getWgs84xy.csv',index=False,mode='w',encoding='utf-8')