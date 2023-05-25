import argparse
import os
import pandas as pd

import openai

from utils import GOOGLE_APPLICATION_CREDENTIALS, GoogleBigQuery

openai.api_key = os.getenv("OPENAI_API_KEY")
openai.organization = os.getenv("ORG_KEY")

def analyze_sentiments(input_sentence):
    response = openai.Completion.create(
        model="text-davinci-003",
        prompt=f"Sentiment analysis of the following text:\n{input_sentence.strip()}\n",
        temperature=0.5,
        max_tokens=3,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=["\\n"]
    )

    return response['choices'][0]['text'].strip()

def main():
    parser = argparse.ArgumentParser(description="CX - MVP and OpenAI Sentiment Analysis Pipeline")
    parser.add_argument("-p","--bigquery-project-name", help="Name of BQ Project", required=True)
    parser.add_argument("-d", "--bigquery-input-dataset", help="Name of BQ Source Dataset", required=True)
    parser.add_argument("-t", "--bigquery-output-dataset", help="Name of BQ Target Dataset", required=True)
    args = parser.parse_args()

    project_name = args.bigquery_project_name
    input_dataset = args.bigquery_input_dataset
    output_dataset = args.bigquery_output_dataset

    gorgias_client = GoogleBigQuery(project_name, input_dataset, GOOGLE_APPLICATION_CREDENTIALS)
    staging_client = GoogleBigQuery(project_name, output_dataset, GOOGLE_APPLICATION_CREDENTIALS)
    
    # getting all eligible user reviews
    # -- this will consider new reviews only
    
    query = """SELECT id, body_text
        FROM `cx-mvp.gorgias.satisfaction_surveys`
        WHERE body_text is not null
        and body_text != ''
    """
    body_text_df = gorgias_client.run_query(query)
    print(f"Got {body_text_df.shape[0]} records to analyze...")

    inferences = []
    for id, body_text in body_text_df.itertuples(index=False):
        sentiment = analyze_sentiments(body_text)
        result = [id, body_text, sentiment]
        inferences.append(result)
        print(result)

    output_df = pd.DataFrame(inferences)
    output_df.columns = ['id','query_text','sentiment']
    staging_client.insert_alter('staging_sentiment_analysis', output_df)
    
if __name__ == "__main__":
    main()