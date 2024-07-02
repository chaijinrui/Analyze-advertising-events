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
    df.query('adId == "1111"', inplace=True)
    # duplicates = df[df.duplicated(subset='adOrderNo', keep=False)]

    logging.info(f'首屏广告的df: \n\n{df.to_string()}')
    # logging.info(f'991的duplicates: \n{duplicates.to_string()}')
    # 以adOrderNo分组，然后统计每个分组中每个adType出现的次数，没有adtype的会被填为0
    grouped_counts = df.groupby('adOrderNo')['adType'].value_counts().unstack(fill_value=0)
    logging.info(f'grouped_counts: \n\n{grouped_counts}')
    logging.info(f'列表列: \n\n{list(grouped_counts.columns)}')

    '''
    申明不同情况的adtype值
    '''
    # 成功
    try:
        n1 = grouped_counts['18']
        n2 = grouped_counts['6']
        n3 = grouped_counts['33']
        n4 = grouped_counts['35']
        n5 = grouped_counts['0']
        n6 = grouped_counts['26']
        n7 = grouped_counts['1']
        # 失败
        n8 = grouped_counts['5']
        n9 = grouped_counts['29']
    except KeyError as e:
        print("异常是：", e)

    # 前提条件 18的数量=成功或失败的数量
    if n1 == n2 or n8 == n1:
        if n3 == n4 == n5 == n6 == n7:
            logging.info("点击了adbutton")
        elif n3 == n4 == n5 == n7 & n6 == 0:
            logging.info("点击了广告")
        elif n3 == n4 == n5 & n6 == n7 == 0:
            logging.info("只展示了广告")
        elif n1 == n8:
            logging.info("竞价失败")
    else:
        logging.warning("广告事件值不正确")

    logging.info(f'缓存池订单号: \n\n{grouped_counts.to_string()}')
