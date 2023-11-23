from flask import Flask, request, jsonify
from cli.junyi import Junyi
from datetime import datetime

app = Flask(__name__)

junyi = Junyi()


@app.route('/login', methods=['POST'])
def login():
    try:
        # Extract input data from the request
        data = request.get_json()

        username = data['username']
        password = data['password']
        print(username, password)

        user_info = junyi.login(username, password)

        return jsonify(user_info), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/register', methods=['POST'])
def register():
    try:
        # Extract input data from the request
        data = request.args
        if not data:
            return jsonify({"error": "No data provided"}), 400

        username = data['username']
        gender = data['gender']
        user_city = data['user_city']
        password = data['password']
        first_login_date_TW = str(datetime.today())

        is_valid = junyi.check_username(username)
        if is_valid:
            user_id = junyi.get_new_id()
            junyi.add_user(user_id, username, gender, first_login_date_TW, user_city, password)
            return jsonify({"info": "Add user successfully"}), 201
        raise Exception("Username existed!")

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/get_topics_by_area', methods=['GET'])
def get_topics_by_area():
    try:
        # Extract input data from the request
        data = request.args
        if not data:
            return jsonify({"error": "No data provided"}), 400

        area = data['area']
        topics = junyi.get_topics_by_area(area)
        return jsonify(topics), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/get_problems_by_area', methods=['GET'])
def get_problems_by_area():
    try:
        # Extract input data from the request
        data = request.args
        if not data:
            return jsonify({"error": "No data provided"}), 400

        area = data['area']
        problems = junyi.get_problems_by_area(area)
        return jsonify(problems), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/get_all_areas', methods=['GET'])
def get_all_areas():
    try:
        # Extract input data from the request
        data = request.args
        problems = junyi.get_all_areas()
        return jsonify(problems), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/get_problems_by_topic', methods=['GET'])
def get_problems_by_topic():
    try:
        # Extract input data from the request
        data = request.args
        if not data:
            return jsonify({"error": "No data provided"}), 400

        topic = data['topic']
        problems = junyi.get_problems_by_topic(topic)
        return jsonify(problems), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/get_problems_by_names', methods=['GET'])
def get_problems_by_names():
    try:
        # Extract input data from the request
        data = request.args
        if not data:
            return jsonify({"error": "No data provided"}), 400

        names = data['names']
        problems = junyi.get_problems_by_names(names)
        return jsonify(problems), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/get_recent_problems_by_user_id', methods=['GET'])
def get_recent_problems_by_user_id():
    try:
        # Extract input data from the request
        data = request.args
        if not data:
            return jsonify({"error": "No data provided"}), 400

        user_id = int(data['user_id'])
        start = data.get("start", "")
        end = data.get("end", "")
        problems = junyi.get_recent_problems_by_user_id(user_id, start, end)
        return jsonify(problems), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/get_total_problem_correct_rate_by_user_id', methods=['GET'])
def get_total_problem_correct_rate_by_user_id():
    try:
        # Extract input data from the request
        data = request.args
        if not data:
            return jsonify({"error": "No data provided"}), 400

        user_id = data['user_id']
        start = data.get("start", "")
        end = data.get("end", "")
        problems = junyi.get_total_problem_correct_rate_by_user_id(user_id, start, end)
        return jsonify(problems), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/get_areas_correct_rate_by_user_id', methods=['GET'])
def get_areas_correct_rate_by_user_id():
    try:
        # Extract input data from the request
        data = request.args
        if not data:
            return jsonify({"error": "No data provided"}), 400

        user_id = data['user_id']
        start = data.get("start", "")
        end = data.get("end", "")
        problems = junyi.get_areas_correct_rate_by_user_id(user_id, start, end)
        return jsonify(problems), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/get_topics_correct_rate_by_user_id', methods=['GET'])
def get_topics_correct_rate_by_user_id():
    try:
        # Extract input data from the request
        data = request.args
        if not data:
            return jsonify({"error": "No data provided"}), 400

        user_id = data['user_id']
        start = data.get("start", "")
        end = data.get("end", "")
        problems = junyi.get_topics_correct_rate_by_user_id(user_id, start, end)
        return jsonify(problems), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/get_problems_correct_rate_by_user_id', methods=['GET'])
def get_problems_correct_rate_by_user_id():
    try:
        # Extract input data from the request
        data = request.args
        if not data:
            return jsonify({"error": "No data provided"}), 400

        user_id = data['user_id']
        start = data.get("start", "")
        end = data.get("end", "")
        problems = junyi.get_problems_correct_rate_by_user_id(user_id, start, end)
        return jsonify(problems), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/write_log', methods=['POST'])
def write_log():
    try:
        # Extract input data from the request
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400

        user_id = data['user_id']
        exercise = data['exercise']
        time_done = int(datetime.now().timestamp()*1000000)
        time_taken = data['time_taken']
        correct = data['correct']

        print(user_id, exercise, time_done, time_taken, correct)
        junyi.write_log(user_id, exercise, time_done, time_taken, correct)
        return jsonify({"info": "write log OK!"}), 201

    except Exception as e:
        print(e)
        return jsonify({"error": str(e)}), 500


@app.route('/statistic_gender', methods=['GET'])
def statistic_gender():
    try:
        res = junyi.statistic_gender()
        return jsonify(res), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/statistic_cities', methods=['GET'])
def statistic_cities():
    try:
        res = junyi.statistic_cities()
        return jsonify(res), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/statistic_problems', methods=['GET'])
def statistic_problems():
    try:
        data = request.args
        start = data.get("start", "")
        end = data.get("end", "")
        res = junyi.statistic_problems(start, end)
        return jsonify(res), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/statistic_topics', methods=['GET'])
def statistic_topics():
    try:
        data = request.args
        start = data.get("start", "")
        end = data.get("end", "")
        res = junyi.statistic_topics(start, end)
        return jsonify(res), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/statistic_areas', methods=['GET'])
def statistic_areas():
    try:
        data = request.args
        start = data.get("start", "")
        end = data.get("end", "")
        res = junyi.statistic_areas(start, end)
        return jsonify(res), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/statistic_correct_rate_by_exercise', methods=['GET'])
def statistic_correct_rate_by_exercise():
    try:
        data = request.args
        if not data:
            return jsonify({"error": "No data provided"}), 400
        start = data.get("start", "")
        end = data.get("end", "")
        exercise = data['exercise']
        res = junyi.statistic_correct_rate_by_exercise(exercise, start, end)
        return jsonify(res), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/statistic_attempts_by_exercise', methods=['GET'])
def statistic_attempts_by_exercise():
    try:
        data = request.args
        if not data:
            return jsonify({"error": "No data provided"}), 400
        start = data.get("start", "")
        end = data.get("end", "")
        exercise = data['exercise']
        res = junyi.statistic_attempts_by_exercise(exercise, start, end)
        return jsonify(res), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/statistic_time_taken_by_exercise', methods=['GET'])
def statistic_time_taken_by_exercise():
    try:
        data = request.args
        if not data:
            return jsonify({"error": "No data provided"}), 400
        start = data.get("start", "")
        end = data.get("end", "")
        exercise = data['exercise']
        res = junyi.statistic_time_taken_by_exercise(exercise, start, end)
        return jsonify(res), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
