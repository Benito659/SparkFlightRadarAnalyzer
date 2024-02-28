from flask import Flask
import os
app = Flask(__name__)

def executecommande(command):
    exit_code = os.system(command)
    if exit_code == 0:
        print("Command executed successfully.")
    else:
        print(f"Error executing command. Exit code: {exit_code}")

@app.route("/launchscriptextractionflight")
def launchscriptextractionflight():
    command = '.\\flight-air\\Scripts\\activate && python flight-extraction-script.py'
    executecommande(command)


@app.route("/launchscripttransformationflight")
def launchscripttransformationflight():
    command = '.\\flight-air\\Scripts\\activate && python flight-transformation-script.py'
    executecommande(command)

    
if __name__ == "__main__":
    current_directory = os.getcwd()
    print("Current Directory:", current_directory)
    app.run(host='0.0.0.0',debug=True)