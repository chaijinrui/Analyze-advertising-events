import logging

'''
记录这些广告位的广告事件
记录每个订单号出现事件的次数
不像缓存池，这些广告位每个订单号只有成功或者失败的情况
根据事件值给本次订单号打上标签
'''


def ad_first(df):
    first_df=df.query('(adId == "1111") | (adId == "1114") | (adId == "231") | (adId == "232") | (adId == "1091")', inplace=False)
    # duplicates = df[df.duplicated(subset='adOrderNo', keep=False)]

    logging.info(f'首个广告的df: \n{first_df.to_string()}')
    # logging.info(f'991的duplicates: \n{duplicates.to_string()}')
    # 以adOrderNo分组，然后统计每个分组中每个adType出现的次数，没有adtype的会被填为0
    grouped_counts = first_df.groupby('adOrderNo')['adType'].value_counts().unstack(fill_value=0)
    # logging.info(f'grouped_counts: \n{grouped_counts}')

    '''
    检验事件是否存在,不存在赋0
    '''
    column_names = ['18', '6', '33', '35', '0', '26', '1', '5', '29']
    for column_name in column_names:
        if column_name not in grouped_counts:
            grouped_counts[column_name] = 0

    # 使用apply()函数将check_ad_events应用到grouped_counts的每一行
    grouped_counts['check'] = grouped_counts.apply(check_ad_events, axis=1)

    logging.info(f'首个广告的事件值: \n{grouped_counts.to_string()}')


# 定义一个函数来检查每行的事件值
def check_ad_events(row):
    # 前提条件 18的数量=成功或失败的数量
    if row['18'] == row['6'] or row['18'] == row['5']:
        if row['35'] == row['0'] == row['26'] == row['1'] and row['33'] != 0:
            return "点击了adbutton"
        elif row['35'] == row['35'] == row['1'] and row['35'] != 0:
            return "点击了广告"
        elif row['35'] == row['0'] and row['35'] != 0:
            return "只展示了广告"
        elif row['18'] == row['5'] != 0:
            return "竞价失败"
    else:
        return "广告事件不正确"
