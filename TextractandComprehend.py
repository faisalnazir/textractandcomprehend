#!/usr/local/bin/python

import boto3
import json
import sys
import time
from os import walk
import argparse

# This is set of utitlity Functions to help the processing
import utils.boe_utils as utils


# These are required parameters
parser = argparse.ArgumentParser(description = 'Required Arguements')
parser.add_argument('--docfolder', '-d', required = True, help = 'The location of where all the PDFs are')
parser.add_argument('--bucket', '-b', required = True, help = 'The location of the S3 bucket you want to use (just the name)')


args = parser.parse_args()


# We are adding the argument values from the command line
bucket = args.bucket
docfolder = args.docfolder

# documents = ['doc1.pdf', 'doc2.pdf', 'doc3.pdf']
documents = [] # This will be populated from upload_files_to_s3
jobs = [] # This will be populate


# These are the Boto3 clients we will use
textract = boto3.client("textract")
comprehend = boto3.client("comprehend")
s3 = boto3.client("s3")


# Start of the main loop.
if __name__ == "__main__" :
    documents = utils.upload_files_to_s3(bucket, docfolder, documents, s3)
    jobs = utils.send_docs_to_textract(documents, jobs, bucket, textract)

    for job in jobs :
        if utils.isJobComplete(job, textract) :
            response = utils.getJobResults(job)
            print(response)
        # Print detected text
        for resultPage in response :
            for item in resultPage["Blocks"] :
                if item["BlockType"] == "LINE" :
                    print('\033[94m' + item["Text"] + '\033[0m')
                    entities = comprehend.detect_entities(LanguageCode = "en", Text = item["Text"])
                    print("\nEntities\n========")
                    with open("entities.txt", "a")as file :
                        for entity in entities["Entities"] :
                            print("{}\t=>\t{}".format(entity["Type"], entity["Text"]))
                            file.write(f'{entity["Type"]},{entity["Text"]},\n')
                            keyphrases = comprehend.detect_key_phrases(LanguageCode = "en", Text = item["Text"])
                            with open("phrases.txt", "a")as file2 :
                                for phrase in keyphrases["KeyPhrases"] :
                                    if "Text" in phrase :
                                        print(phrase)
                                        print(f"Phrase\t{phrase['Text']} with confidence = {phrase['Score']}")
                                        file2.write(f"{phrase['Text']},{phrase['Score']}\n")
                                        file2.write(json.dumps(phrase))
                                        file2.write("\n")


    # Upload the final output files into S3 for futher processing
    s3.upload_file("entities.txt", "fsnz-boe", "entities/CQCentities.txt")
    print(f'Uploaded Entities to S3')
    s3.upload_file("phrases.txt", "fsnz-boe", "phrases/CQCphrases.txt")
    print(f'Uploaded Key Phrases to S3')


# TODOs
# # Add Elastic Search
# # Add Neptune + Graph Visualisation(Needs some thought)
