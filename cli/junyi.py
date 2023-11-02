import os
from datetime import datetime

from databricks import sql
from databricks.sql.types import Row
from dotenv import load_dotenv


def get_time_query(start="", end="", variable="time_done", has_and=True):
    if start == "" and end == "":
        return ""
    elif start != "" and end == "":
        start_tsp = int(datetime.strptime(start, '%Y-%m-%d').timestamp() * 1000000)
        return f" AND {variable} >= {start_tsp}" if has_and else f" {variable} >= {start_tsp}"
    elif start == "" and end != "":
        end_tsp = int(datetime.strptime(end, '%Y-%m-%d').timestamp() * 1000000)
        return f" AND {variable} <= {end_tsp}" if has_and else f" {variable} <= {end_tsp}"
    else:
        start_tsp = int(datetime.strptime(start, '%Y-%m-%d').timestamp() * 1000000)
        end_tsp = int(datetime.strptime(end, '%Y-%m-%d').timestamp() * 1000000)
        return f" AND {variable} BETWEEN {start_tsp} AND {end_tsp}" \
            if has_and else f" {variable} BETWEEN {start_tsp} AND {end_tsp}"


class Junyi:
    def __init__(self, env_file=None) -> None:
        if env_file is None:
            env_file = "./.env"
        load_dotenv(env_file)
        host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        http_path = os.getenv("DATABRICKS_HTTP_PATH")
        access_token = os.getenv("DATABRICKS_TOKEN")
        self._connection = sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=access_token)

    def _query(self, query):
        cursor = self._connection.cursor()
        res: list[Row] = cursor.execute(query)
        results = [row.asDict(recursive=True) for row in res]
        return {
            "results": results,
            "count": len(results)
        }

    def _query_list(self, query, key):
        cursor = self._connection.cursor()
        res: list[Row] = cursor.execute(query)
        results = [row.asDict(recursive=True) for row in res]
        return {
            "results": [result[key] for result in results],
            "count": len(results)
        }

    def login(self, username, password):
        query = f"""SELECT user_id, username, password FROM user_table
        WHERE username = "{username}"
        """
        res = self._query(query)
        if res['count'] == 0:
            raise Exception("Your username does not exist!")
        else:
            password_ref = str(res['results'][0]['password'])
            if password_ref == password:
                return res
            else:
                raise Exception("Wrong password!")

    def get_new_id(self):
        q = """SELECT max(user_id) as max_user_id FROM user_table"""
        res = self._query(q)
        return res['results'][0]['max_user_id']

    def check_username(self, username):
        query = f"""SELECT username FROM user_table
        WHERE username = "{username}"
        """
        res = self._query(query)
        if res['count'] == 0:
            return True
        else:
            raise Exception("Existed username!")

    def add_user(self, user_id, username, gender, first_login_date_TW, user_city, password, points=0, badges_cnt=0,
                 user_grade=0):
        q = f"""INSERT INTO user_table (user_id, username, gender, points, badges_cnt, first_login_date_TW, user_grade, user_city, password)
        VALUES ({user_id+1}, '{username}', '{gender}', 0, 0, Date("{first_login_date_TW}"), 0, "{user_city}", "{password}");
        """
        self._query(q)

    def get_topics_by_area(self, area):
        q = f"""SELECT DISTINCT topic FROM problem
        WHERE area = "{area}" ;
                """
        return self._query_list(q, "topic")

    def get_problems_by_area(self, area):
        q = f"""SELECT name, prerequisites,  seconds_per_fast_problem, topic, area, question, answer, rationale ,
        options, documents  FROM problem WHERE area = "{area}" 
        """
        return self._query(q)

    def get_all_areas(self):
        q = f"""SELECT area FROM problem GROUP BY area
        """
        return self._query_list(q, "area")

    def get_problems_by_topic(self, topic):
        q = f"""SELECT name, prerequisites,  seconds_per_fast_problem, topic, area, question, answer, rationale ,
        options, documents FROM problem WHERE topic = "{topic}" 
        ORDER BY name;"""
        return self._query(q)

    def get_problems_by_names(self, names: str):
        name_list = names.split("|")
        name_list = [f"'{name}'" for name in name_list]
        names = " , ".join(name_list)
        names = f"({names})"
        q = f"""SELECT name, prerequisites,  seconds_per_fast_problem, topic, area, question, answer, rationale ,options, documents FROM problem
        WHERE name IN {names}
        ORDER BY name;
        """
        return self._query(q)

    def get_recent_problems_by_user_id(self, user_id, start="", end=""):
        time_query = get_time_query(start, end)
        q = f"""SELECT user_id, exercise,  problem_type, time_done, time_taken, time_taken_attempts, 
        correct, count_attempts, hint_used, count_hints, hint_time_taken_list, earned_proficiency, points_earned   FROM log_table
        WHERE user_id = {user_id} {time_query}
        ORDER BY time_done DESC;"""
        return self._query(q)

    def get_total_problem_correct_rate_by_user_id(self, user_id, start="", end=""):
        time_query = get_time_query(start, end)
        q = f"""SELECT COUNT(*) as total, SUM(CASE WHEN correct = 1 THEN 1 ELSE 0 END) as correct FROM log_table
        WHERE user_id = {user_id} {time_query}"""
        return self._query(q)

    def get_areas_correct_rate_by_user_id(self, user_id, start=None, end=None):
        time_query = get_time_query(start, end)
        q = f"""SELECT MAX(log_table.time_done) as last_try, COUNT(log_table.time_done) as num_done, SUM(CASE WHEN log_table.correct = 1 THEN 1 ELSE 0 END) as correct, problem.area FROM log_table 
        RIGHT JOIN problem ON log_table.exercise=problem.name
        WHERE user_id = {user_id} {time_query} 
        GROUP BY problem.area ORDER BY last_try DESC"""
        return self._query(q)

    def get_topics_correct_rate_by_user_id(self, user_id, start=None, end=None):
        time_query = get_time_query(start, end)
        q = f"""SELECT MAX(log_table.time_done) as last_try, COUNT(log_table.time_done) as num_done, SUM(CASE WHEN log_table.correct = 1 THEN 1 ELSE 0 END) as correct, problem.topic FROM log_table
        RIGHT JOIN problem ON log_table.exercise=problem.name
        WHERE user_id = {user_id} {time_query} 
        GROUP BY problem.topic ORDER BY last_try DESC"""
        return self._query(q)

    def get_problems_correct_rate_by_user_id(self, user_id, start=None, end=None):
        time_query = get_time_query(start, end)
        q = f"""SELECT MAX(log_table.time_done) as last_try, COUNT(log_table.time_done) as num_done, SUM(CASE WHEN log_table.correct = 1 THEN 1 ELSE 0 END) as correct, problem.name FROM log_table
        RIGHT JOIN problem ON log_table.exercise=problem.name
        WHERE user_id = {user_id} {time_query} 
        GROUP BY problem.name ORDER BY last_try DESC"""
        return self._query(q)

    def write_log(self, user_id, exercise, time_done, time_taken, correct):
        # time_done must be in seconds timestamp
        # time_taken second
        q = f"""INSERT INTO log_table (user_id, exercise, suggested, review_mode, time_done, time_taken, correct)
        VALUES ({user_id},'{exercise}', false, false, {time_done}, {time_taken}, {correct});"""
        return self._query(q)

    def statistic_gender(self):
        q = f"""SELECT count(*) as count, gender FROM user_table
        GROUP BY gender"""
        return self._query(q)

    def statistic_cities(self):
        q = f"""SELECT count(user_id) as num_user, user_city FROM user_table
        GROUP BY user_city ORDER BY num_user DESC"""
        return self._query(q)

    def statistic_problems(self, start=None, end=None):
        time_query = get_time_query(start, end, variable="log_table.time_done", has_and=False)
        if time_query:
            q = f"""SELECT count(user_id) as num_user, exercise FROM log_table
            WHERE {time_query}
            GROUP BY exercise ORDER BY num_user DESC 
            """
        else:
            q = f"""SELECT count(user_id) as num_user, exercise FROM log_table
            GROUP BY exercise ORDER BY num_user DESC"""
        return self._query(q)

    def statistic_topics(self, start=None, end=None):
        time_query = get_time_query(start, end, variable="log_table.time_done", has_and=False)
        if time_query:
            q = f"""SELECT  topic, count(user_id) as num_done  FROM log_table
            JOIN problem ON problem.name = log_table.exercise
            WHERE {time_query}
            GROUP BY topic ORDER BY num_done DESC 
            """
        else:
            q = f"""SELECT  topic, count(user_id) as num_done  FROM log_table 
            JOIN problem ON problem.name = log_table.exercise
            GROUP BY topic ORDER BY num_done DESC """
        return self._query(q)

    def statistic_areas(self, start=None, end=None):
        time_query = get_time_query(start, end, variable="log_table.time_done", has_and=False)
        if time_query:
            q = f"""SELECT  area, count(user_id) as num_done  FROM log_table 
            JOIN problem ON problem.name = log_table.exercise
            WHERE {time_query}
            GROUP BY area ORDER BY num_done DESC 
            """
        else:
            q = f"""SELECT  area, count(user_id) as num_done  FROM log_table 
            JOIN problem ON problem.name = log_table.exercise
            GROUP BY area ORDER BY num_done DESC """
        return self._query(q)

    def statistic_correct_rate_by_exercise(self, exercise, start=None, end=None):
        time_query = get_time_query(start, end, variable="time_done", has_and=True)
        q = f"""SELECT round(correct/total, 1) as correct_rate, COUNT(user_id) as num_user FROM 
        (SELECT user_id, COUNT(*) as total, SUM(CASE WHEN correct = 1 THEN 1 ELSE 0 END) as correct FROM log_table
        WHERE exercise = "{exercise}" {time_query}
        GROUP BY user_id)
        GROUP BY correct_rate 
        """
        return self._query(q)

    def statistic_attempts_by_exercise(self, exercise, start=None, end=None):
        time_query = get_time_query(start, end, variable="time_done", has_and=True)
        q = f"""SELECT num_trial, count(user_id) as num_user FROM (SELECT  user_id, count(*) as num_trial  FROM 
        log_table WHERE exercise = "{exercise}" {time_query} GROUP BY user_id) WHERE num_trial BETWEEN 1 AND 10 
        GROUP BY num_trial ORDER BY num_user
        """
        return self._query(q)

    def statistic_time_taken_by_exercise(self, exercise, start=None, end=None):
        time_query = get_time_query(start, end, variable="time_done", has_and=True)
        q = f"""SELECT  time_taken, count(user_id) as num_user  FROM log_table 
        WHERE exercise = "{exercise}" {time_query}
        GROUP BY time_taken ORDER BY num_user DESC LIMIT 20
        """
        return self._query(q)

    # def statistic_review_by_exercise(self, exercise, start=None, interval=None, limit=5):
    #     pass
