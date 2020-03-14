from pyspark.sql import SparkSession
from pyspark.sql.functions import max, sum, rank, count, lit, datediff, round, expr, col
from pyspark.sql import Window


class MailRu:

    def __init__(self):
        self.spark = SparkSession.builder.appName('abc').getOrCreate()

    def load_data_csv(cls, spark, file):
        try:
            return spark.read.format("csv") \
                .option("sep", ";") \
                .option("inferSchema", "true") \
                .option("header", "true") \
                .load(file)
        except Exception as e:
            print(f"Error while reading csv file {e}")

    def sql_task(self):
        """
        SQL
        Нужно посчитать DAU (day active users - уникальные пользователи которые входили в игру за день)
        по дням за последние 30 дней.
        Без дополнительных условий;
        Ограничиваем только тех пользователей у которых был login за последние 24 часа;
        Учитываем только тех пользователей которые суммарно заплатили за последние 30 дней более 100$
        """
        logins = self.load_data_csv(self.spark, "logins.csv")
        logins.createOrReplaceTempView("logins")

        logouts = self.load_data_csv(self.spark, "logouts.csv")
        logouts.createOrReplaceTempView("logouts")

        payments = self.load_data_csv(self.spark, "payments.csv")
        payments.createOrReplaceTempView("payments")

        df = self.spark.sql("""
         select to_date(login_datetime,'yyyy-mm-dd') as login_date, 
            l.user_id
            from (select user_id, login_datetime 
                    from logins 
                    where login_datetime >= date_add(current_timestamp(), -1) ) l
            left join (select 
                        user_id, sum(payment_sum) p_sum
                        from payments 
                        where payment_datetime >= date_add(current_timestamp(), -30)
                        group by user_id
                        having sum(payment_sum) > 100
            )
            p on p.user_id=l.user_id
            where login_datetime >= date_add(current_timestamp(), -30) and p_sum is not null
            group by to_date(login_datetime,'yyyy-mm-dd'), l.user_id
            order by 1 desc, 2
        """)
        df.show()

    def main_character(self):
        """
        Нужно узнать, какой “основной” класс персонажа у каждого юзера.
        То есть, за какой класс он играл больше всего времени за весь период.
        На выходе должны получить таблицу user_id; character
        """

        df = self.load_data_csv(self.spark, "user_classes.csv")
        df_agg = df.groupBy("user_id", "character"). \
            agg(
            sum("session_time").alias("ch_ses"))

        windowSpec = Window.orderBy("ch_ses").partitionBy("user_id")
        df_favorite_character = df_agg.withColumn("character_rank",
                                                  rank().over(windowSpec)) \
            .filter("character_rank = 1") \
            .drop("ch_ses", "character_rank").orderBy("user_id")

        df_favorite_character.show()

    def calculate_arpu(self):
        """
        Имеем таблицу с платежами: user_id, pay_dt, pay_sum и таблицу с регистрациями:
        user_id; reg_dt. Нужно посчитать ARPU (Average Revenue Per User)
        для каждого дня жизни этих пользователей.
        День жизни пользователей -- количество дней между датой платежа и датой регистрации пользователя.
        На выходе нужно получить следующую таблицу:
        diff; arpu, где diff - количество дней между датой платежа и датой регистрации,
        оно же день жизни пользователей.
        Таким образом эта таблица будет показывать,
        сколько в среднем каждый пользователь приносит денег на 0 день жизни, на 1 день жизни и так далее.
        Важный момент: стоит обратить внимание, что далеко не все пользователи должны попадать в выборку для расчета.
        Например, считая ARPU за 90й день жизни будет некорректно брать тех юзеров,
        которые зарегистрировали вчера, т.к. они просто еще попросту не прожили 90 дней и никак не могли заплатить.
        """

        df_user_reg = self.load_data_csv(self.spark, "user_reg.csv")
        df_payments = self.load_data_csv(self.spark, "payments.csv")

        df_payments_agg = df_payments.withColumn("pay_dt", expr("to_date(payment_datetime)")) \
            .groupBy("pay_dt", "user_id").agg(sum("payment_sum").alias("rev"))

        df_user_reg_agg = df_user_reg.withColumn("reg_dt", expr("to_date(reg_dt)")) \
            .join(df_payments_agg, df_payments_agg.user_id == df_user_reg.user_id) \
            .withColumn("diff", datediff(df_payments_agg.pay_dt, col("reg_dt"))) \
            .groupBy("diff").agg((round(sum("rev") / col("diff"), 2)).alias("arpu"))

        df_user_reg_agg.show()


