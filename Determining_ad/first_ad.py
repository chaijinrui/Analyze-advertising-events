'''
首屏广告 包括1111、231、242、252、1091
不包括红包页的首屏
不同参数每回合的订单号不变

判断日志正确标准：每个订单号的事件值如下
前提：
18=6+5；
6=33=35=0；
判断同一订单号的28、3、4...的值
没有26和1。 展示
只有1，点击
有26和1 点击button
只有18和5失败

'''
import logging


def ad_first(df):
    df.query('adId == "1111" or adId == "231" or adId == "242" or adId == "252"or adId == "1091"', inplace=True)
    # duplicates = df[df.duplicated(subset='adOrderNo', keep=False)]

    logging.info(f'首屏广告的df: \n{df.to_string()}')
    # logging.info(f'991的duplicates: \n{duplicates.to_string()}')
    # 以adOrderNo分组，然后统计每个分组中每个adType出现的次数，没有adtype的会被填为0
    grouped_counts = df.groupby('adOrderNo')['adType'].value_counts().unstack(fill_value=0)

    '''
    申明不同情况的adtype值
    '''
    boolen1=

    # 判断18=6+5； 6=33=35=0； 为前提条件
    if (grouped_counts['18'] == (grouped_counts['3'] + grouped_counts['4'])) & (
            grouped_counts['6'] == grouped_counts['33'] == grouped_counts['35'] == grouped_counts['0']):
        # 添加一列来表示判断结果
        if '26' not in grouped_counts.columns & '1' not in grouped_counts.columns:
            grouped_counts['check'] = "展示成功"
        elif '26' not in grouped_counts.columns & '1' in grouped_counts.columns:
            grouped_counts['check'] = "点击成功"
        elif '26' in grouped_counts.columns & '1' in grouped_counts.columns:
            grouped_counts['check'] = "点击adbutton成功"
        elif '' not in grouped_counts.columns and '4' not in grouped_counts.columns:
            grouped_counts['check'] = "没有3和4"
            logging.warning("没有3和4")

        elif '28' in grouped_counts.columns:
            grouped_counts['check'] = "没有28"
        logging.warning("没有28")

    else:
        logging.warning("广告事件值不正确")

    logging.info(f'缓存池订单号: \n{grouped_counts.to_string()}')
