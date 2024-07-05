
import logging

'''
同一个订单号的请求，可能有3也可能有4的情况
判断广告事件是否符合预期，符合则返回True，否则返回其他情况
'''
def ad_pool(df):
    poor_df = df.query('adId == "991" or adId == "992"', inplace=False)
    # duplicates = df[df.duplicated(subset='adOrderNo', keep=False)]
    logging.info(f'缓存池的的df: \n{poor_df.to_string()}')
    # logging.info(f'991的duplicates: \n{duplicates.to_string()}')

    # 判断相同订单号，28的数量=3+4的数量
    grouped_counts = poor_df.groupby('adOrderNo')['adType'].value_counts().unstack(fill_value=0)
    if '3' in grouped_counts.columns and '4' in grouped_counts.columns:
        # 添加一列来表示判断结果，这里我们用True表示条件满足，False表示不满足
        grouped_counts['check'] = (grouped_counts['28'] == (grouped_counts['3'] + grouped_counts['4']))
    elif '3' not in grouped_counts.columns:
        grouped_counts['check'] = (grouped_counts['28'] == (grouped_counts['4']))
    elif '4' not in grouped_counts.columns:
        grouped_counts['check'] = (grouped_counts['28'] == (grouped_counts['3']))
    elif '3' not in grouped_counts.columns and '4' not in grouped_counts.columns:
        grouped_counts['check'] = "没有3和4"
        logging.warning("没有3和4")
    elif '28' in grouped_counts.columns:
        grouped_counts['check'] = "没有28"
        logging.warning("没有28")

    logging.info(f'缓存池订单号: \n{grouped_counts.to_string()}')
