import re
import pandas as pd
import numpy as np
import logging
import joblib


class ModelProvider:
    dutch_content = []
    meta_model = None
    law_titles = None
    model_logger = None

    no_category_text = ["Wij konden uw tekst niet naar een categorie indelen","","",""]
    
    def __init__(
        self, cleanup_file_path: str, model_file_path: str, law_titles_file_path: str
    ):
        logging.info("initializing model provider")
        self._cleanup_file_path = cleanup_file_path
        self._model_file_path = model_file_path
        self._law_titles_file_path = law_titles_file_path
        logging.info("loading file dependencies")
        self._load_text()
        self._load_model()
        self._load_law_titles()
        pass

    def _load_text(self):
        logging.info("loading dutch texts")
        self.dutch_content = []
        with open(self._cleanup_file_path, "r") as file:
            content = file.readlines()
            # you may also want to remove whitespace characters like `\n` at the end of each line
            content = [x.strip() for x in content]
            content[0] = "aan"
            for i in content:
                self.dutch_content.append("\\b" + i + "\\b")
            self.dutch_content.append("//[a-z]" + "\.")

    def _load_model(self):
        logging.info("loading model")
        self.meta_model = joblib.load(self._model_file_path)

    def _load_law_titles(self):
        logging.info("loading law titles")
        self.law_titles = pd.read_csv(self._law_titles_file_path,index_col="id")
        self.law_titles = self.law_titles.fillna("Empty")

    # prepares input text to be handled by predictive model
    def prepare_text(self, text_input):
        text = text_input.lower()
        text = re.sub("\\n", "", text)
        text = re.sub("[0-9]+", "", text)
        for i in self.dutch_content:
            text = re.sub(i, "", text)
        text = text.strip(" ")
        text = re.sub("\s\s+", " ", text)
        return text

    # formats a list of predictions to be ready for frontend consumption -- LEGACY
    def format_predictions(self, results: list[str]) -> list[str]:
        return list(map(lambda result: f"<div>{result}</div>", results))

    # returns a list of (unformatted) predictions
    def predict_categories(self, text_input: str) -> list[str]:
        logging.info(f"predicting categories for: {text_input}")
        #print(self.law_titles.loc["Highest_Aanbestedingswet 2012"])
        try:
            matrix = self.meta_model.predict([text_input])[0]
            #detected = np.where(matrix == 1)[1]
            detected = [matrix]
            if len(detected) < 1:
                raise ValueError("no categories detected")
            logging.info("detected categories")
            return [self.law_titles.loc[detected][x][0] for x in ["output","areaLevel1","areaLevel2","areaLevel3"]]
        except ValueError:
            logging.info("no category detected")
            return [self.no_category_text]
        except Exception as e:
            logging.info(e)
            logging.info("other error, no category detected")
            return [self.no_category_text]

    # handle prediction e2e including IO formatting
    def predict(self, text_input: str):
        text = self.prepare_text(text_input)
        predictions = self.predict_categories(text)
        predictions[0] = f"<div>{predictions[0]}</div>" # The first prediction should be formatted with </div> to be ready for front-end consumption
        return predictions

if __name__ == "__main__":
    pass
