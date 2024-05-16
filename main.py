from flask import Flask, request, jsonify
from flask_cors import CORS
from produce_explanation import get_explanations

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

@app.route('/getExplanations', methods=['POST'])
def get_items():
    data = request.get_json()

    # Extract device ID and items from the request
    device_id = data.get('deviceId')
    items = data.get('items', [])

    # Check if the device ID exists
    explanation = dict()
    for item_id in items:
        res = get_explanations(device_id=device_id, item_id=item_id)
        if res:
            explanation[item_id] = res
    # Filter items based on the device
    return jsonify(result=explanation)


if __name__ == '__main__':
    app.run(debug=True)
