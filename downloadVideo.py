# coding=utf-8
import threading
import MySQLdb
from datetime import datetime
import time
from log import logger
from subprocess import call

IDM = r'C:\Program Files (x86)\Internet Download Manager\IDMan.exe'
DownPath = r'E:\stuff\Spider\video\js7tv'
gapTime=60*2

def get_con():
    host = "127.0.0.1"
    port = 3306
    logsdb = "video"
    user = "root"
    password = "root"
    con = MySQLdb.connect(host=host, user=user, passwd=password, db=logsdb, port=port, charset="utf8")
    return con


def calculate_time():
    now = time.mktime(datetime.now().timetuple())-gapTime
    result = time.strftime('%Y%m%d%H%M%S00', time.localtime(now))
    logger.info(result+"*****")
    return result


def get_data():
    select_time = calculate_time()
    logger.info("select time:"+select_time)
    sql = u"select fldRecdId,shipin,fldUrlAddr from video.视频表单 where fldRecdId >"+"'"+select_time+"'order by fldRecdId desc"
    conn = get_con()
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results


#def send_email(content):
    # sender = "sender_monitor@163.com"
    # receiver = ["rec01@163.com", "rec02@163.com"]
    # host = 'smtp.163.com'
    # port = 465
    # msg = MIMEText(content)
    # msg['From'] = "sender_monitor@163.com"
    # msg['To'] = "rec01@163.com,rec02@163.com"
    # msg['Subject'] = "system error warning"
    #
    # try:
    #     smtp = smtplib.SMTP_SSL(host, port)
    #     smtp.login(sender, '123456')
    #     smtp.sendmail(sender, receiver, msg.as_string())
    #     logger.info("send email success")
    # except Exception, e:
    #     logger.error(e)

def downloadVideo(content):
    for r in content:
        OutPutFileName=r[0].encode('utf-8') + '.mp4'
        DownUrl=r[1]
        # content += r[1].encode('utf-8') + '\n'
        if DownUrl is not None and len(DownUrl) > 0:
            call([IDM, '/d', DownUrl, '/p', DownPath, '/f', OutPutFileName, '/n', '/a'])
            call([IDM, '/s'])
            logger.info(r[0].encode('utf-8') +","+r[2].encode('utf-8'))
            time.sleep(.2)
        else:
            logger.info("Wrong  "+r[0].encode('utf-8') +","+r[2].encode('utf-8'))

    logger.info("Start Downloading...")


def task():
    while True:
        logger.info("Monitor Running")
        results = get_data()
        if results is not None and len(results) > 0:
            content = ""
            logger.info("Add to Download Queue")
            # send_email(content)
            downloadVideo(results)
            logger.info(content)
        time.sleep(gapTime)


def run_monitor():
    monitor = threading.Thread(target=task)
    monitor.start()


if __name__ == "__main__":
    run_monitor()
