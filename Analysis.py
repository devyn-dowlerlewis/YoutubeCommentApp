from flair.models import TextClassifier
from flair.data import Sentence
import ipywidgets

def analyse_sentiment(cul_df, cul_comdf, df_anal, comdf_anal):
    print("Loading Pre-Trained Sentiment Models. Please Wait.")
    tagger = TextClassifier.load('sentiment')
    tagger2 = TextClassifier.load('sentiment-fast')
    print("Sentiment Models Loaded")
    positive_coms = 0
    ambiguous_coms = 0
    negative_coms = 0
    for index, row in cul_comdf.iterrows():
        sentence = Sentence(row['comment'])

        tagger.predict(sentence)
        slow_rslt = sentence.labels[0].to_dict()['confidence']
        if sentence.labels[0].to_dict()['value'] == 'NEGATIVE':
            slow_rslt = 1 - slow_rslt

        tagger2.predict(sentence)
        fast_rslt = sentence.labels[0].to_dict()['confidence']
        if sentence.labels[0].to_dict()['value'] == 'NEGATIVE':
            fast_rslt = 1 - fast_rslt

        if (slow_rslt + fast_rslt > 1.4) and (abs(slow_rslt-fast_rslt < 0.35)):
            positive_coms += 1
            print(f"{fast_rslt}, {slow_rslt} | Positive: {row['comment']}")
        elif (slow_rslt + fast_rslt < 0.6) and (abs(slow_rslt-fast_rslt < 0.35)):
            negative_coms += 1
        else:
            ambiguous_coms += 1
            #print(f"{fast_rslt}, {slow_rslt} | Ambiguous: {row['comment']}")

        comdf_anal = comdf_anal.append({'key': row['key'], 'flair_sentiment_slow': slow_rslt, 'flair_sentiment_fast': fast_rslt}, ignore_index=True)


    print(f"Positive: {positive_coms}, Ambiguous: {ambiguous_coms}, Negative: {negative_coms}")




    return cul_df, cul_comdf, df_anal, comdf_anal