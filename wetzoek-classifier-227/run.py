import logging

from query_classifier.query_classifier import ModelProvider

logging.basicConfig(level=logging.DEBUG)
mp = ModelProvider(
    "./sample_data/dutch.txt",
    "./sample_data/20230125model.pkl",
    "./sample_data/20230206titles.csv",
)
for i in ["ontslag op staande voet", "werk", "other","Wat kost een boerderij?","wat is de straf op witwassen?"]:
    predictions = mp.predict(i)
    logging.debug("=== OUTPUT ===")
    logging.debug(predictions)
