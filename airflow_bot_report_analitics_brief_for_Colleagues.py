
# ----------------------------------------------------------------------------------------
#             ЗАДАЧА
# ----------------------------------------------------------------------------------------
# Запрос от коллег :  наладить автоматическую отправку аналитической сводки в телеграм каждое утро!

# Отобразить текст с информацией о значениях ключевых метрик за предыдущий день/метрик за предыдущие 7 дней
# Добавить  в отчете следующие ключевые метрики: 
# DAU 
# Просмотры
# Лайки
# CTR
# Бог и   Airflow в помощь.





import pandas as pd
import pandahouse as ph
import numpy as np

import seaborn as sns
import matplotlib.pyplot as plt

from datetime import datetime, timedelta

import io
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# ----------------------------------------------------------------------------------------
#             ЧАСТЬ ПОДКЛЮЧЕНИЯ
# ----------------------------------------------------------------------------------------

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

#  параметры для задач дагов
default_args = {
    'owner': 'b.vasilev-10',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2023, 1, 18),
}

chat_id = -850804180
my_token = '5888788890:AAFQuzQtcZq-hTSjfj6IlnCVzulaHhBbU2E'
bot = telegram.Bot(token=my_token)

schedule_interval = '0 11 * * *' #  11 утра

# ----------------------------------------------------------------------------------------
#             ПИШЕМ ОСНОВНОЙ ДАГ
# ----------------------------------------------------------------------------------------


#  ДАГ
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def bot_report_vasilev_7_1():
    
    @task()
    def metrics_last_day():
    
        # последний день
        query = '''SELECT max(toDate(time)) as day,
                sum(action = 'like') as likes,
                sum(action = 'view') as views, 
                count(DISTINCT user_id) AS "DAU",
                likes/views as CTR
                FROM simulator_20221220.feed_actions
                WHERE toDate(time) = yesterday();
                '''
        data_metrics_last_day = ph.read_clickhouse(query, connection=connection)
        return data_metrics_last_day

    
    @task()
    def metrics_last_week():
    
        # за прошедшую неделею
        metrics_last_week = '''SELECT toDate(time) as day,
                   sum(action = 'like') as likes,
                   sum(action = 'view') as views,
                   count(DISTINCT user_id) AS "DAU", 
                   likes/views as CTR
                FROM simulator_20221220.feed_actions
                WHERE toDate(time) > today() -8 AND toDate(time) < today()
                GROUP BY day'''
        data_metrics_last_week = ph.read_clickhouse(data_metrics_last_week, connection=connection)
        return data_metrics_last_week
# ----------------------------------------------------------------------------------------
#             ЧАСТЬ ОТПРАВКИ СООБЩЕНИЙ В ГРУППУ
# ----------------------------------------------------------------------------------------


    @task()
    def send_message(data_metrics_last_day, chat_id):

        date_max = data_metrics_last_day.day.to_string(index=False)
        views = int(data_metrics_last_day.views)
        likes = int(data_metrics_last_day.likes)
        DAU = int(data_metrics_last_day.DAU)
        CTR = round(float(data_metrics_last_day.CTR), 5)

        mess = f'''
                METRICS FOR LAST DAY
                Key metrics for *{date_max}*
                Views - {views:,}
                Likes - {likes:,}
                DAU - {DAU:,}
                CTR - {CTR}'''

        bot.sendMessage(chat_id=chat_id, text=mess, parse_mode= 'Markdown')

    # шлем этодело
    @task()
    def send_photo_week(data_metrics_last_week, chat_id):
        fig, axes = plt.subplots(2, 2, figsize=(18, 12))
        fig.suptitle('Тенденция за 7 дней', fontsize=25)


        sns.lineplot(ax = axes[1, 0], data = data_metrics_last_week, x = 'day', y = 'views')
        axes[1, 0].set_title('Views')
        axes[1, 0].grid()

        sns.lineplot(ax = axes[1, 1], data = data_metrics_last_week, x = 'day', y = 'likes')
        axes[1, 1].set_title('likes')
        axes[1, 1].grid()

        sns.lineplot(ax = axes[0, 0], data = data_metrics_last_week, x = 'day', y = 'DAU')
        axes[0, 0].set_title('DAU')
        axes[0, 0].grid()

        sns.lineplot(ax = axes[0, 1], data = data_metrics_last_week, x = 'day', y = 'CTR')
        axes[0, 1].set_title('CTR')
        axes[0, 1].grid()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'picture.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)    
    
    
    #засылаем за последний день инфу пометрикам
    data_metrics_last_day = metrics_last_day()
    send_message(data_metrics_last_day, chat_id)
    #засылаем за последнюю неделю инфу пометрикам
    data_metrics_last_week = metrics_last_week()
    send_photo_week(data_metrics_last_week, chat_id)
    
bot_report_vasilev_7_1 = bot_report_vasilev_7_1()