'''
分析流程：
·提出问题及问题分析
·数据清洗及预处理
·可视化分析与探索

一、提出问题及问题分析
1、提出问题
直击————如何找一个性价比高的房子？
2、问题分析
·房源区域数量分布
·房租价格分布
·面积-租金 分析
·待定
二、数据清洗及预处理
·概览：数据统计、分布情况
·清洗脏数据->缺失值、异常值
·数据合并及重塑
三、可视化分析及探索
'''
import random
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import re
import random


def layout_cleaning(area):
    if area < 60:
        return '1室1厅1卫'
    elif area < 80:
        return '2室1厅1卫'
    elif area < 120:
        return '3室2厅1卫'
    elif area < 180:
        return '4室2厅2卫'
    elif area < 260:
        return '5室2厅3卫'
    else:
        return '6室1厅1卫'


def subway_cleaning(x):
    if x == '[]':
        return '距地铁至少1.2公里'
    else:
        return x.split("'",2)[1]



def floor_cleaning(floor):
    if floor < 1 / 3:
        return '低楼层/56层'
    elif floor < 2 / 3:
        return '中楼层/56层'
    else:
        return '高楼层/56层'


if __name__ == '__main__':
    # 将爬虫获得的数据读取进来
    df_house = pd.read_csv('house_info.csv')
    '''
    以下为数据清洗及预处理过程：
    '''
    # # 查看行数据重复情况，下面显示无重复
    # print(df_house.duplicated().sum())
    # # 先看一下数据分布情况，可以看出：一共有24个特征，52242条数据，其中只有community有293条缺失，但此字段在后续分析中没太大用处，此处不填充
    # print(df_house.info())
    # # 开始清洗数据
    # # 看一下前10条数据，逐一分析24个特征
    # print(df_house.head(10))
    # #字段house_id、district_and_community没啥问题，且在后续分析用处不大，此处不处理。district字段代表行政区，看看有木有非行政区名称的信息
    # print(df_house['district'].value_counts())
    # # 发现有1662条非行政区名称的信息，有几个词的数量还比较多，以下将这几个比较多的进行处理，个位数的词直接删除即可:
    # # 查看"敏捷"的小区名name,其均为"广州敏捷绿湖首府"这个小区，随便找个地图看，小区在增城，手动处理,community这里也可以处理一下，后续探索性分析可以细化分析一下
    # print(df_house.loc[df_house.district == '敏捷','name'].value_counts())
    df_house.loc[df_house.district == '敏捷', ['district', 'community']] = ['增城', '石滩镇']
    # # 同理，其他的也处理一下
    df_house.loc[df_house.district == '雅居', ['district', 'community']] = ['番禺', '雅居乐']
    df_house.loc[df_house.district == '绿地', ['district', 'community']] = ['黄埔', '知识城']
    df_house.loc[df_house.district == '融创', ['district', 'community']] = ['花都', '花城街']
    df_house.loc[df_house.district == '星河', ['district', 'community']] = ['番禺', '星河湾']
    df_house.loc[df_house.district == '海伦', ['district', 'community']] = ['增城', '沙村']
    df_house.loc[df_house.district == '设计', ['district', 'community']] = ['白云', '黄边']
    df_house.loc[df_house.district == '亚运', ['district', 'community']] = ['番禺', '亚运城']
    df_house.loc[df_house.district == '碧桂', ['district', 'community']] = ['番禺', '南浦']
    df_house.loc[df_house.district == '渔人', ['district', 'community']] = ['番禺', '洛溪']
    df_house.loc[df_house.district == '龙湖', ['district', 'community']] = ['黄埔', '科学城']
    df_house.loc[df_house.district == '奥林', ['district', 'community']] = ['番禺', '洛溪']
    df_house.loc[df_house.district == '时代', ['district', 'community']] = ['南沙', '金洲']
    df_house.loc[df_house.district == '大道', ['district', 'community']] = ['白云', '京溪']
    df_house.loc[df_house.district == '紫郡', ['district', 'community']] = ['荔湾', '西朗']
    df_house.loc[df_house.district == '机务', ['district', 'community']] = ['越秀', '三元里']
    df_house.loc[df_house.district == '大学', ['district', 'community']] = ['番禺', '大学城']
    df_house.loc[df_house.district == ') ', ['district', 'community']] = ['黄埔', '知识城']
    df_house.loc[df_house.district == '） ', ['district', 'community']] = ['白云', '人和']
    # # 删除district中非行政区的脏数据对应的行,步骤：1-列出行政区的列表；2-找出符合行政区列表中的所有行并取反，获得对应索引并转化为列表，用drop()删除对应索引即可
    district = ['天河', '越秀', '番禺', '增城', '白云', '黄埔', '海珠', '南沙', '荔湾', '花都', '从化']
    drop_index = df_house.loc[~df_house.district.isin(district)].index.tolist()
    df_house.drop(index=drop_index, inplace=True)
    # # name、maintenance_time、rental_menthon后面分析用处不大，不处理
    # # layout 为房间布局，规整格式应该是x室x厅x卫，此处用正则验证一下,发现下面的代码报错，即存在非规整格式的layout值
    # print(df_house['layout'].map(lambda str: re.findall(r'\d+室\d厅\d卫',str)[0]).shape[0])
    # # 粗略浏览，发现个别layout存在包含"未知"的户型,此处数据清洗按照面积大小划分,故先将面积字段house_area清洗一下,转为浮点型
    df_house['house_area'] = df_house['house_area'].str.replace('㎡', '').astype('float')
    # #layout字段中的脏数据区间在[25,430]间,根据layout分组聚合后求均值,结合日常经验,可以对脏数据手工分组,构造函数layout_cleaning()
    # print(df_house.loc[df_house.layout.str.contains('未知'), 'house_area'].sort_values())
    # print(df_house.groupby(['layout']).house_area.agg('mean').sort_values().index.tolist())
    # print(df_house.groupby(['layout']).house_area.agg('mean').sort_values().values.tolist())
    df_house.loc[df_house.layout.str.contains('未知'), 'layout'] = df_house.loc[df_house.layout.str.contains('未知'), 'house_area'].apply(lambda x: layout_cleaning(x))
    # # 再用正则验证一下，输出52196行，没有报错，说明layout列的数据已经规整
    # print(df_house['layout'].map(lambda str: re.findall(r'\d+室\d厅\d卫', str)[0]).shape[0])
    # # 此处可以把x室x厅x卫分离出来，后面探索分析时可以细化分析
    df_house['num_bedroom'] = df_house['layout'].apply(lambda x: x.split('室')[0]).astype('int')
    df_house['num_living_room'] = df_house['layout'].apply(lambda x: x.split('室')[1].split('厅')[0]).astype('int')
    df_house['num_toilet'] = df_house['layout'].apply(lambda x: x.split('厅')[1].strip('卫')).astype('int')
    # # orientation特征即朝向，先汇总看看有什么类型->发现有很多是有多个朝向的，应该是对应多个房间的朝向，此处可以取第一个朝向，后面可以探索分析
    # print(df_house['orientation'].value_counts())
    df_house['orientation'] = df_house['orientation'].apply(lambda x: x.split('/')[0])
    # #floor特征为楼层，用以下代码看了下特征值，其呈现出三种形态，1、高中低楼层/x层；2、x/x层；3、地下室/x层(这种极少数，视作低楼层)，处理思路及步骤:1、地下室/x层处理成低楼层/x层；2、x/x层按照 x÷x的浮点型得数，在floor_cleaning()函数中清洗成相对楼层；3、上述两个步骤中将数据规整成高中低楼层/x层的格式，然后统一分割取相对楼层
    # print(df_house['floor'].value_counts().index.tolist())
    df_house.loc[df_house.floor.str.contains('地下室'), 'floor'] = '低楼层/9层'
    df_house.loc[~df_house.floor.str.contains('楼层'), 'floor'] = df_house.loc[~df_house.floor.str.contains('楼层'), 'floor'].apply(lambda x: float(x.split('/')[0]) / float(x.split('/')[1].strip('层'))).apply(lambda x: floor_cleaning(x))
    df_house['floor'] = df_house['floor'].apply(lambda x: x.split('/')[0])
    # # 以下代码验证，floor特征仅有高楼层、低楼层、中楼层三个值，此特征处理成功
    # print(df_house['floor'].value_counts())
    # # 验证elevator特征，均规整,同理验证parking、water、electricity、gas、pay_methon
    # print(df_house['elevator'].value_counts())
    # # 上面已知道rent_of_month、deposit为int型，此处可以列出新特征进行探索分析，以契合我们提出的问题：每平方面积的月租金，rent_per_area = rent_of_month / house_area
    df_house['rent_per_area'] = df_house[['rent_of_month', 'house_area']].apply(lambda x: round(x['rent_of_month'] / x['house_area'], 0), axis=1)
    # # the_nearest_subway,the_nearest_subway_distance地铁特征,先看一下特征值构成，发现形式上（实际上是字符串）有些为空列表，有些为多元素列表，观察多元素列表，其为按距离升序排序,且距离最大为1199m,可见房源1.2公里内有地铁才算近地铁
    # print(df_house[['the_nearest_subway','the_nearest_subway_distance']].head(20))
    # # 处理思路：1、新增一列：是否近地铁(is_near_subway);2、若不近地铁的取最近的地铁站
    df_house['is_near_subway'] = df_house['the_nearest_subway'].apply(lambda x: '否' if x == '[]' else '是')
    df_house['the_nearest_subway'] = df_house['the_nearest_subway'].apply(lambda x: subway_cleaning(x))
    df_house['the_nearest_subway_distance'] = df_house['the_nearest_subway_distance'].apply(lambda x: subway_cleaning(x))
    df_house.loc[df_house.the_nearest_subway_distance.str.contains('m'),'the_nearest_subway_distance'].apply(lambda x: x.replace('m','')).astype('int')
    # # 最后看下经纬度，是浮点型且非空非na，不用处理
    # print(df_house['longitude'].isna().sum())
    # # 将清洗完的数据保存下来
    # df_house.to_csv('houses_info_cleaned.csv',mode='w',encoding='utf-8',index=False)
    # # 描述性统计->可用jupyter notebook 处理起来会更方便些
    # # 对house_area箱线图可视化,发现有较多异常值，异常值即默认为【均值±1.5倍四分位差】，但1.5倍可以视实际情况调整,从描述性统计中看出中位数为80.5，上四分位数102，若按1.5倍算，最大面积也只有222平方，但不排除有些房子是三五百平的，此处先降序看下500平面积以上的房子有多少,发现有63个是500以上的，也不乏一些1000以上的超大豪宅，拿到他们的链接去看下
    # plt.boxplot(df_house['house_area'],showmeans=True,autorange=True,flierprops={'marker':'D','markerfacecolor':'red'})
    # plt.show()
    # print(df_house.loc[df_house.house_area > 500,['house_area','name']].sort_values(by='house_area',ascending=False))
    # # 可以看出63个>500平方的房源中，除个别是大别野外，多是办公场地出租或者异常数据，鉴于我等社畜一般不会租这种房子（租不起），此处的处理方法是直接删除
    drop_index2 = df_house.loc[df_house.house_area > 500].index.tolist()
    df_house.drop(index=drop_index2,inplace=True)
    # # 现在看下月租的箱线图
    # plt.boxplot(df_house['rent_of_month'],whis=4,showmeans=True, autorange=True,flierprops={'marker': 'D', 'markerfacecolor': 'red'})
    # plt.show()
    # # 再将whis调到4后还是有很多异常值，同理按价格降序看下是什么样的房子？月租降序top 50 租金都到5.7w了，贫穷限制了我的想象，先看下租金5w的面积及区位
    # print(df_house['rent_of_month'].sort_values(ascending=False).head(50))
    # print(df_house.loc[df_house.rent_of_month > 50000,['rent_of_month','house_area','district','community']].sort_values(by='rent_of_month',ascending=False).head(50))
    # # 打扰了，一看都是像珠江新城、东山口等富人区的大面积，这个租金无可厚非，数据不用处理,再看看单价有木有比较离谱的
    # print(df_house[['name','rent_of_month','house_area','rent_per_area']].sort_values(by='rent_per_area',ascending=False).head(50))
    # # 不看不知道，一看吓一跳，有些夸张到单价在1000以上，400以上的也有好多，考虑到一来是脏数据，二来我等社畜租不起这么贵的房子，下面300+挺多的，所以决定将500及以上的数据作为脏数据删除
    # drop_index3 = df_house.loc[df_house.rent_per_area > 400].index.tolist()
    # df_house.drop(index=drop_index3,inplace=True)
    # # # 清洗后的数据保存
    # df_house.to_csv('house_info_cleaned_finally.csv',index=False,mode='w',encoding='utf-8')
    # print('数据清洗完毕，且已保存')


'''
至此，数据预处理及清洗已经完成
'''
