
# ----------------------------------------------------------------------------------------
#             ЧАСТЬ ПОДКЛЮЧЕНИЯ
# ----------------------------------------------------------------------------------------

# Попросили более  детальный отчет   по работе всего приложения.
# Приоложение социальной сети, в  отчете должна быть информация и по ленте новостей, и по сервису отправки сообщений. 
# Метрики продумывал самостоятельно. Графики для упрощения восприятия.  В целом содержит информацию о работе приложения  для  бизнес отдела.



# ----------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------





# *** это не кончательный варинта, я долго парился с гитом
# и аирфлоу, не хватило времени.

#     КОРОТКО,ПРИНЦИП
#Суть следующая - сначала человек смотрит общие показатели, более старшего порядка 
# и отвечающих на вопрос "сколько".

#Далее, смотри картину недавнего периода(неделя) и отвечающего на вопрос "сколько"
# но в уже  1) более конкретно, по каждому сервису.

#И далее, наконец, смотрит картину отвечающую на вопрос "где".
# 1)  В разибивке по каждому сервису
# 2) разбивка по отельным параметрами кажлого сервиса, если наступит Трындец, то один из
# показателей это отобразит.


#ПС. но наверно не все показатели я сюда поставил, я просто взял из своих дашбордов.
# а так меня посещает мысль, я мог  6-8 показателей для одного сервиса  соединить  по 
# 2 или 3 показателя в метрику отношений # , как  СTR . 
# А после , просто расчитать ДИ (интревал в заиввисости от того что хотим замечать) 
# и получать алерты на колебания ЗА предел ДоверительногоИнтервала - и дальше,уже смотреть
#  по отдельности,  данные ,  которые входят # в  эту метрику отношений




from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context
import requests
import pandas as pd
import numpy as np
import pandahouse
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.pyplot import figure
import telegram
import io

connection1 = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20221220',
    'user': 'student',
    'password': 'dpo_python_2020'}

default_args = {
    'owner': 'b.vasilev-10',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2023, 1, 20),
}

schedule_interval = '0 11 * * *'

chat_id = -850804180
bot_token = '5888788890:AAFQuzQtcZq-hTSjfj6IlnCVzulaHhBbU2E'
bot = telegram.Bot(token = bot_token)

#-----------------------------------------------
#показатели 30 days и больше
#-----------------------------------------------

@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def bot_report_vasilev_7_2():
    
# здесь пусть будут  осн показатели 
# (DAU
# WAU 
# retention)
# 30 ДНЕЙ  и старше

#     хапросы к КХ
    @task()
    def dau(chat_id, connection1):
        query4 =                                             
        """SELECT  toDate(time) as day,
        COUNT(DISTINCT user_id) AS DAU
        FROM simulator_20221220.feed_actions
        WHERE toDate(time) BETWEEN yesterday() - 29 AND yesterday()
        GROUP BY day"""
        df4 = pandahouse.read_clickhouse(query4, connection = connection1)
        return df4


    @task()
    def wau(chat_id, connection1):
        query5 = """SELECT  toMonday(time) as week,
        COUNT(DISTINCT user_id) AS WAU
        FROM simulator_20221220.feed_actions
        WHERE toDate(time) BETWEEN yesterday() - 59 AND yesterday()
        GROUP BY week"""
        df5 = pandahouse.read_clickhouse(query5, connection = connection1)
        return df5


    @task()
    def retention(chat_id, connection1):
        query6 = """SELECT toStartOfDay(toDateTime(this_week)) AS __timestamp,
            status AS status,
            max(num_users) AS "MAX(num_users)"
            FROM
                (SELECT this_week,
                    previous_week, -uniq(user_id) as num_users,
                    status
                FROM
                    (SELECT user_id,
                        groupUniqArray(toMonday(toDate(time))) as weeks_visited,
                        addWeeks(arrayJoin(weeks_visited), +1) this_week,
                        if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status,
                        addWeeks(this_week, -1) as previous_week
                    FROM simulator_20221220.feed_actions
                    GROUP BY user_id)
                WHERE status = 'gone'
                GROUP BY this_week,
                    previous_week,
                    status
                HAVING this_week != addWeeks(toMonday(today()), +1)
                UNION ALL SELECT this_week,
                    previous_week,
                    toInt64(uniq(user_id)) as num_users,
                    status
                FROM
                    (SELECT user_id,
                        groupUniqArray(toMonday(toDate(time))) as weeks_visited,
                        arrayJoin(weeks_visited) this_week,
                        if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status,
                        addWeeks(this_week, -1) as previous_week
                    FROM simulator_20221220.feed_actions
                    GROUP BY user_id)
                GROUP BY this_week,
                    previous_week,
                    status) AS virtual_table
            GROUP BY status,
                    toStartOfDay(toDateTime(this_week))
            ORDER BY "MAX(num_users)" DESC
            LIMIT 1000;
                                     """
        df6 = pandahouse.read_clickhouse(query6, connection = connection1)
        return df6

        
#  РИСУЕМ query4
        fig1, ax1 = plt.subplots()
        fig1.set_size_inches(18, 10, forward=True)
        x1 = df4.iloc[1:]['DAU']
        y1 = df4.iloc[1:]['day']
        ax1.bar(y1, x1, color = 'blue')

        ax1.set_title('Количество уникальных пользователей за предыдущие 30 дней по дням', fontsize = 20)
        ax1.set_xlabel('dau')
        ax1.set_ylabel('dates')
        

# В КАРТИНК
        month_dau = io.BytesIO()
        plt.savefig(month_dau)
        month_dau.seek(0)
        month_dau.name = 'month_dau.png'
        plt.close()
        

#рисуем query 5
        fig3, ax3 = plt.subplots()
        fig3.set_size_inches(18, 10, forward=True)
        x3 = df5['WAU']
        y3 = df5['week']
        ax3.bar(y3, x3, color = 'blue')

        ax3.set_title('Количество уникальных пользователей за предыдущие 2 мес по неделям', fontsize = 20)
        ax3.set_xlabel('WAU')
        ax3.set_ylabel('week')
        
# В КАРТИНКУ 
        month_wau = io.BytesIO()
        plt.savefig(month_wau)
        month_wau.seek(0)
        month_wau.name = 'month_wau.png'
        plt.close()
        

# рисуем query6
#тУт из этой статьи  не получилось https://towardsdatascience.com/100-stacked-charts-in-python-6ca3e1962d2b
        sns.barplot(data = df6.sort_values('__timestamp', ascending=False) \
            , x = '__timestamp', y = 'MAX(num_users)', hue = 'status', order = df6.__timestamp.unique())
# plt.savefig('retention.png')

#  В КАРТИНКУ        
        retention = io.BytesIO()
        plt.savefig(retention)
        retention.seek(0)
        retention.name = 'retention.png'
        plt.close()


#  ШЛЁМ
        bot.sendPhoto(chat_id = chat_id, photo = month_dau)
        bot.sendPhoto(chat_id = chat_id, photo = month_wau)
        bot.sendPhoto(chat_id = chat_id, photo = retention)

#-----------------------------------------------
#среднесроные показатели, 7 days
#-----------------------------------------------
#Динамика ежедневного притока аудитории +
#кол-во уникальных пользователей сервиса сообщений +
#кол-во уникальных пользователей ленты новостей, 7 дней


    @task()
    def distinct_users_messages(chat_id, connection1):
# ЗАПРОСЫ
#кол-во уникальных пользователей сервиса сообщений
        query1 = """SELECT toStartOfDay(toDateTime(date)) AS __timestamp,
                    count(DISTINCT user_id) AS "COUNT_DISTINCT(user_id)"
                    FROM
                    (SELECT toDate(mess.time) AS date,
                            mess.user_id AS user_id
                    FROM simulator_20221220.feed_actions AS feed
                    RIGHT JOIN simulator_20221220.message_actions AS mess ON feed.user_id = mess.user_id
                    AND feed.user_id IS NULL) AS virtual_table
                    WHERE __timestamp BETWEEN yesterday() - 6 AND yesterday()
                    GROUP BY toStartOfDay(toDateTime(date))
                    ORDER BY "COUNT_DISTINCT(user_id)" DESC
                    LIMIT 1000;"""
        df1 = pandahouse.read_clickhouse(query1, connection = connection1)
        return df1

        
# кол-во уникальных пользователей ленты новостей
    @task()
    def distinct_users_feed(chat_id, connection1):
        query2 = """SELECT toStartOfDay(toDateTime(time)) AS __timestamp,
                count(DISTINCT user_id) AS "количество пользоватлей ленты новостей"
                    FROM
                    (SELECT toDate(feed.time) AS time,
                            feed.user_id AS user_id
                    FROM simulator_20221220.feed_actions as feed
                    LEFT JOIN simulator_20221220.message_actions as mess ON feed.user_id = mess.user_id
                    AND mess.user_id IS NULL) AS virtual_table
                    WHERE __timestamp BETWEEN yesterday() - 6 AND yesterday()

                    GROUP BY toStartOfDay(toDateTime(time))
                    ORDER BY "количество пользоватлей ленты новостей" DESC
                    LIMIT 1000;
                    """
        df2 = pandahouse.read_clickhouse(query2, connection = connection1)
        return df2


#  Динамика ежедневного притока аудитории 
    @task()
    def new_come_users(chat_id, connection1):
        query3 = """ SELECT  date_of_reg,
                count(users) as new_come_user

                    FROM(
                    SELECT
                    DISTINCT(user_id ) as users,
                    MIN(toDate(time)) as date_of_reg
                                    
                    FROM simulator_20221220.feed_actions 
                    GROUP BY users
                    )
                    WHERE date_of_reg BETWEEN yesterday() - 6 AND yesterday() 
                    GROUP BY date_of_reg
                    ORDER by date_of_reg DESC"""
        
        df3 = pandahouse.read_clickhouse(query3, connection = connection1)
        return df3
        
# РИСУЕМ
        figure = plt.figure()
        figure.set_size_inches(18.5, 10.5, forward=True)
        
        ax1 = figure.add_subplot(2, 2, 1)
        ax2 = figure.add_subplot(2, 2, 2)
        ax3 = figure.add_subplot(2, 2, 3)
        ax4 = figure.add_subplot(2, 2, 4)
        
        ax1.bar(df1['__timestamp'], df1['COUNT_DISTINCT(user_id)'], color = 'blue')
        ax2.bar(df2['__timestamp'], df2['количество пользоватлей ленты новостей'], color = 'blue')
        ax3.bar(df1['date_of_reg'], df1['new_come_user'], color = 'blue')
        
        
        ax1.set_title('лента новостей')
        ax2.set_title('сервис ‘сообщений')
        ax3.set_title('Количество новых пользователей')
        
        figure.suptitle('Значения метрик  за предыдущие 7 дней', fontsize=28)
        
        four_charts = io.BytesIO()
        plt.savefig(four_charts)
        four_charts.seek(0)
        four_charts.name = 'four_charts.png'
        plt.close()
        
        
        bot.sendPhoto(chat_id = chat_id, photo = four_charts)
        


#-----------------------------------------------
# показатели реагирования, сегодня, лента
#-----------------------------------------------
#активность по ос + 
#колл постов по часам + 
#Колличество Просмотров и Лайков по часам

    @task()
    def indicators_of_the_attention_lenta_service_os(chat_id, connection1):
# ЗАПРОСЫ
#кол-во  активных пользователей сервиса ленты в РАЗРЕЗЕ ОС по часам
        query11 = """SELECT toStartOfHour(toDateTime("Hour")) AS __timestamp,
                    os AS os,
                    max(activity) AS "MAX(activity)"
                    FROM
                        (SELECT COUNT (user_id) AS activity,
                            toStartOfHour(toDateTime(time)) AS Hour,
                            action,
                            os
                        FROM simulator_20221220.feed_actions
                        group by action,
                            Hour,
                            os
                        having action == 'view') AS virtual_table
                    WHERE "Hour" >= toDateTime(yesterday())
                    AND "Hour" <= toDateTime(now())
                    GROUP BY os,
                        toStartOfHour(toDateTime("Hour"))
                    ORDER BY "MAX(activity)" DESC
                    LIMIT 1000;"""
        df11 = pandahouse.read_clickhouse(query11, connection = connection1)
        return df11
        
# кол-во постов ленты новостей по часам

    @task()
    def indicators_of_the_attention_lenta_service_posts(chat_id, connection1):
        query22 = """SELECT toStartOfHour(toDateTime(time)) AS __timestamp,
                        count(post_id) AS "Колличество постов"
                    FROM simulator_20221220.feed_actions
                    WHERE time >= toDateTime(yesterday())
                    AND time <=toDateTime(now())
                    GROUP BY toStartOfHour(toDateTime(time))
                    ORDER BY "Колличество постов" DESC
                    LIMIT 10000;
                    """
        df22 = pandahouse.read_clickhouse(query22, connection = connection1)
        return df22

    @task
    def indicators_of_the_attention_lenta_service_likes_views(chat_id, connection1):
#  Колличество Просмотров и Лайков по часам
        query33 = """SELECT toStartOfHour(toDateTime(time)) AS __timestamp,
                        action AS action,
                        count(user_id) AS "Колличество лайков"
                    FROM simulator_20221220.feed_actions
                    WHERE time >= toDateTime(yesterday())
                    AND time <= toDateTime(now())
                    GROUP BY action,
                        toStartOfHour(toDateTime(time))
                    ORDER BY "Колличество лайков" DESC
                    LIMIT 10000;"""
        
        df33 = pandahouse.read_clickhouse(query33, connection = connection1)
        return df33

# РИСУЕМ
        figure = plt.figure()
        figure.set_size_inches(18.5, 10.5, forward=True)
        
        ax1 = figure.add_subplot(2, 2, 1)
        ax2 = figure.add_subplot(2, 2, 2)
        ax3 = figure.add_subplot(2, 2, 3)
        ax4 = figure.add_subplot(2, 2, 4)
      
        ax1.bar(df11['__timestamp'], df11['MAX(activity)'], color = 'blue')
        ax2.bar(df22['__timestamp'], df22['Колличество постов'], color = 'blue')
        ax3.bar(df33['__timestamp'], df33['Колличество лайков'], color = 'blue')
        
        
        ax1.set_title('кол-во  активных пользователей сервиса ленты в РАЗРЕЗЕ ОС по часам')
        ax2.set_title('кол-во постов ленты новостей по часам')
        ax3.set_title('Колличество Просмотров и Лайков по часам')
        
        figure.suptitle('вчера сегодня лента', fontsize=28)
        
        four_charts = io.BytesIO()
        plt.savefig(four_charts)
        four_charts.seek(0)
        four_charts.name = 'four_charts.png'
        plt.close()
        
        
        bot.sendPhoto(chat_id = chat_id, photo = four_charts)



#-----------------------------------------------
# показатели реагирования, сегодня, сервис сообщений
#-----------------------------------------------
#активность по ос + 
#колл постов по часам + 
#Колличество Просмотров и Лайков по часам

# ЗАПРОСЫ
#кол-во  активных пользователей сервиса cоббщение в РАЗРЕЗЕ ОС по часам

    @task()
    def indicators_of_the_attention_meassages_service(chat_id, connection1):
        query111 = """SELECT toStartOfHour(toDateTime(time)) AS Hour,
                    os,
                    count(DISTINCT user_id) AS h
                    FROM simulator_20221220.message_actions
                    where Hour <= toDateTime(now())
                    AND Hour >= toDateTime(yesterday())
                    GROUP by Hour,os
                    ORDER BY h DESC
                    LIMIT 1000;"""
        df111 = pandahouse.read_clickhouse(query111, connection = connection1)
        return df111
        
# кол-во отосланных сообщений сервиса сообщений по часам 
    @task()
    def indicators_of_the_attention_meassages_service_sent(chat_id, connection1):
        query222 = """SELECT toStartOfDay(toDateTime(time)) AS Hour,
                        count(user_id) AS sum_user
                    FROM simulator_20221220.message_actions
                    GROUP BY Hour

                    LIMIT 1000;
                    """
        df222 = pandahouse.read_clickhouse(query222, connection = connection1)
        return df222

    @task()
    def indicators_of_the_attention_meassages_service_recieved(chat_id, connection1):
# кол-во принятх сообщений сервиса сообщений по часам 
        query333 = """SELECT toStartOfDay(toDateTime(time)) AS Hour,
                        count(reciever_id) AS sum_user
                    FROM simulator_20221220.message_actions
                    GROUP BY Hour

                    LIMIT 1000;
                    """
        df333 = pandahouse.read_clickhouse(query333, connection = connection1)
        return df333
        
# РИСУЕМ
        figure = plt.figure()
        figure.set_size_inches(18.5, 10.5, forward=True)
        
        ax1 = figure.add_subplot(2, 2, 1)
        ax2 = figure.add_subplot(2, 2, 2)
        ax3 = figure.add_subplot(2, 2, 3)
        ax4 = figure.add_subplot(2, 2, 4)
        
        ax1.bar(df111['Hour'], df111['h'], color = 'blue')
        ax2.bar(df222['Hour'], df222['sum_user'], color = 'blue')
        ax3.bar(df333['Hour'], df333['sum_user'], color = 'blue')
        
        
        ax1.set_title('кол-во  активных пользователей сервиса cоббщение в РАЗРЕЗЕ ОС по часам')
        ax2.set_title('кол-во отосланных сообщений сервиса сообщений по часам ')
        ax3.set_title('кол-во принятх сообщений сервиса сообщений по часам ')
        
        figure.suptitle('вчера сегодня лента', fontsize=28)
        
        four_charts_2 = io.BytesIO()
        plt.savefig(four_charts_2)
        four_charts_2.seek(0)
        four_charts_2.name = 'four_charts_2.png'
        plt.close()
        
        
        bot.sendPhoto(chat_id = chat_id, photo = four_charts_2)


    dau(chat_id, connection1)
    wau(chat_id, connection1)
    retention(chat_id, connection1)
    distinct_users_messages(chat_id, connection1)
    distinct_users_feed(chat_id, connection1)
    new_come_users(chat_id, connection1)
    indicators_of_the_attention_lenta_service_os(chat_id, connection1)
    indicators_of_the_attention_lenta_service_posts(chat_id, connection1)
    indicators_of_the_attention_lenta_service_likes_views(chat_id, connection1)
    indicators_of_the_attention_meassages_service(chat_id, connection1)
    indicators_of_the_attention_meassages_service_recieved(chat_id, connection1)

    
bot_report_vasilev_7_2 = bot_report_vasilev_7_2()

