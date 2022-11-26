from flask import *
from Training_Model import train_model
from flask_cors import CORS, cross_origin
from prediction_from_model import prediction

app = Flask(__name__)
CORS(app)

@app.route("/",methods=["GET"])
@cross_origin()
def home():
    return render_template("home.html")

@app.route("/predict",methods=["POST"])
@cross_origin()
def predict():
    if request.method == "POST":
        try:
            if request.form:
                data_req = dict(request.form)
                data = data_req.values()
                data = [ list(data) ]
                pred = prediction(data)
                result = pred.predict()
                if result == 1:
                    result = "Yes"
                else:
                    result = "No"
                return render_template('result.html', result = result)
            else:
                return None
        except Exception as E:
            return Response(E)

    else:
        return Response("Error occurred")


@app.route("/train",methods = ["POST"])
@cross_origin()
def training():
    try:
        data = request.json["filepath"]
        train_mod = train_model(data)
        train_mod.training_model()
        return Response("Training Successfull!!")

    except ValueError:
        return Response("Error Occurred! ::" + str(ValueError))
    except KeyError:
        return Response("Error Occurred! ::" + str(KeyError))
    except Exception as e:
        return Response("Error Occurred! Error::" + str(e))

if __name__ == "__main__":
    app.run(debug = True)
