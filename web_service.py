from flask import Flask, request

app = Flask(__name__)

@app.route('/normalize', methods=["POST"])
def normalize():
    try:
        req = request.get_json()
        message = req['message']
        # Normalizar eliminando los espacios que sobran
        normalized = ' '.join(message.split())
        return normalized, 201
    except Exception as e:
        return {"error": str(e)}, 415
    
app.run(debug=False, host="0.0.0.0", port="5000")
