'''
缓存池的逻辑，目前是根据106竞价金额判断，请求池为991还是992
看oppo 十万个冷知识1.4.9的逻辑
同一回合的请求，不同参数、状态，订单号都是相同的
'''
import logging


def ad_pool(df):
    poor_df=df.query('adId == "991" or adId == "992"', inplace=False)
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
