import subprocess
import time
import logging
from urllib.parse import unquote_plus

import pandas as pd
import re

'''
连接手机、获取日志、断开日志
'''
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# 启动adb logcat命令并将其输出定向到管道      标签为LOGCAT_CONSOLE且日志等级为INFO的日志
logging.info("启动adb logcat命令")
# 清除日志
subprocess.Popen(['adb', '-s', 'VWMF6PRSUGVC4LGM', 'logcat', '-c'])
logcat = subprocess.Popen(['adb', '-s', 'VWMF6PRSUGVC4LGM', 'logcat', '-s', 'LOGCAT_CONSOLE'], stdout=subprocess.PIPE)
log_list = []

while True:

    # 从管道中读取一行输出
    try:
        line = logcat.stdout.readline().decode('utf-8')
    except KeyboardInterrupt:
        logging.info("用户中断了程序")

    # 如果没有更多的输出，则退出循环
    if not line:
        break

    # 如果line不为空，都是则把所有日志输出到一个log_df
    if line:
        log_list.append(line)

    # 判断广告事件
    if "==== 广告事件 ===" in line:
        # 动态提添加到列表
        log_list.append(line)

    # 打印error
    if "E/" in line:
        logging.error(line)

logging.info(f'获取到的广告日志为：{log_list}')
# 确保关闭adb logcat命令
logcat.kill()
# 调用dataframe格式化函数
# get_log_dataframe(log_list)


'''
把所有日志都组成一个dataframe

'''
log_pattern = r"(\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+\w+\s+\w+\s+(\w)\s+(LOGCAT_CONSOLE):\s+(.*?)\r\n"
ve_value_pattern = r've=(.*?)(&|$)'
tem_list = []
for log_line in log_list:
    match = re.match(log_pattern, log_line)
    if match:
        timestamp, level, _, message = match.groups()
        '''
        打印版本号
        '''
        if "sdk2_adconfig_req_data" in message:
            decoded_url_with_plus = unquote_plus(message)
            match = re.search(ve_value_pattern, decoded_url_with_plus)
            if match:
                ve_value = match.group(1)
                logging.info(f've_value: {ve_value}')
        tem_list.append([timestamp, level, message])
    else:
        logging.warning(f'log_list没有匹配到日志: {log_line}')
columns = [
    'Timestamp',
    'Level',
    'Message'
]
df = pd.DataFrame(tem_list, columns=columns)
# 将Timestamp列转换为datetime格式
df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%m-%d %H:%M:%S.%f')
# 确保Timestamp列的显示格式为'%m-%d %H:%M:%S.%f'
df['Timestamp'] = df['Timestamp'].dt.strftime('%m-%d %H:%M:%S.%f')
logging.info(f'df: \n{df.to_string()}')


'''
打印error
'''
err_row = df.query('Level == "E"')
if err_row.empty:
    logging.info('没有报错')
err_row_message = err_row['Message']
logging.info(f'err_row_message: {err_row_message}')


'''
广告加载出错的时候
'''

# 正则表达式
ad_pattern = r'(\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+(\d+)\s+(\d+)\s+V\s+LOGCAT_CONSOLE:\s+.*\sadId\[(\d+)\],\s+state\[(.*?)\],\s+adParam\[(.*?)\]?,\s+adOrderNo\[(.*?)\],\s+adType\[(\d+)\],\s+sp\[(.*?)\]'
data_list = []
# 匹配日志行
for log_line in log_list:
    match = re.match(ad_pattern, log_line)
    if match:
        timestamp, _, _, adId, state, adParam, adOrderNo, adType, sp = match.groups()
        # logging.info(f'匹配到日志: {match.groups()}')
        data_list.append([timestamp, adId, state, adParam, adOrderNo, adType, sp])
    else:
        logging.warning(f'没有匹配到事件日志: {log_line}')
# 将数据转换为DataFrame
columns = ['Timestamp', 'adId', 'State', 'adParam', 'adOrderNo', 'adType', 'sp']
df = pd.DataFrame(data_list, columns=columns)
# 将Timestamp列转换为datetime格式
df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%m-%d %H:%M:%S.%f')
# 确保Timestamp列的显示格式为'%m-%d %H:%M:%S.%f'
df['Timestamp'] = df['Timestamp'].dt.strftime('%m-%d %H:%M:%S.%f')

# 增加缺失值标记 Missing
df2 = df.applymap(lambda x: 'Missing' if x.strip() == '' else x)
logging.info(f'df2: \n{df2.to_string()}')

# 判断adOrderNo是否正确（之前出现过bug：adOrderNo非本广告位的订单号）
df2.apply(lambda row: logging.warning(f'adOrderNo和adId不符: {row["adOrderNo"]}, {row["adId"]}')
if row["adOrderNo"].split('_')[0] != row["adId"] else None, axis=1)

# 按时间戳列倒序排序
# df_sorted = df2.sort_values(by='Timestamp', ascending=False)
# logging.info(f'df_sorted: \n{df_sorted.to_string()}')
