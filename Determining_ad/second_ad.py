import logging

'''
记录这些广告位的广告事件
记录每个订单号出现事件的次数
不像缓存池，这些广告位每个订单号只有成功或者失败的情况
根据事件值给本次订单号打上标签
'''


def ad_first(df):
    df.query(
        '(adId == "1112") | (adId == "1113") | (adId == "1115") | (adId == "1116") | (adId == "241")| (adId == "251")| (adId == "242")| (adId == "252")| (adId == "1092")| (adId == "1093")',
        inplace=True)
    # duplicates = df[df.duplicated(subset='adOrderNo', keep=False)]

    logging.info(f'第二屏广告的df: \n\n{df.to_string()}')
    # logging.info(f'991的duplicates: \n{duplicates.to_string()}')
    # 以adOrderNo分组，然后统计每个分组中每个adType出现的次数，没有adtype的会被填为0
    grouped_counts = df.groupby('adOrderNo')['adType'].value_counts().unstack(fill_value=0)
    # logging.info(f'grouped_counts: \n{grouped_counts}')

    '''
    检验事件是否存在，创建一个字典，事件和事件出现次数对应起来
    '''
    values = {}
    column_names = ['18', '6', '33', '35', '0', '26', '1', '5', '29']
    column_codes = ['n1', 'n2', 'n3', 'n4', 'n5', 'n6', 'n7', 'n8', 'n9']
    for column_name, column_code in zip(column_names, column_codes):
        if column_name not in grouped_counts:
            grouped_counts[column_name] = 0
        values[column_code] = grouped_counts[column_name]
    # logging.info(f'grouped_counts: \n{grouped_counts}')
    # logging.info(f'grouped_counts[column_name]: \n{grouped_counts["18"]}')
    # logging.info(f'values[n]:{values["n1"]}')

    # 前提条件 18的数量=成功或失败的数量
    if values['n1'].sum() == values['n2'].sum() or values['n1'].sum() == values['n8'].sum():
        if (values['n3'].sum() == values['n4'].sum() == values['n5'].sum() == values['n6'].sum() == values[
            'n7'].sum()) & values['n3'].sum() != 0:
            grouped_counts['check'] = "点击了adbutton"
        elif (values['n4'].sum() == values['n5'].sum() == values['n7'].sum()) & values['n4'].sum() != 0:
            grouped_counts['check'] = "点击了广告"
        elif (values['n4'].sum() == values['n5'].sum()) & values['n4'].sum() != 0:
            grouped_counts['check'] = "只展示了广告"
        elif values['n1'].sum() == values['n8'].sum():
            grouped_counts['check'] = "竞价失败"
    else:
        grouped_counts['check'] = "广告事件不正确"

    logging.info(f'首个广告的事件值: \n{grouped_counts.to_string()}')
