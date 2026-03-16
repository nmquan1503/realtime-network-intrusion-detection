from flask import Flask, jsonify, render_template
import random, time

app = Flask(__name__)

@app.route("/data")
def get_data():
    return jsonify({
        "data": [[int(time.time()), random.randint(0,100)]],
        "predict_data": [[int(time.time()), random.randint(0,100)]]
    })

@app.route("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)