from os import walk
import time
import boto3
import json
import sys


def upload_files_to_s3(bucket, docfolder, documents, s3):
    """
    This function will talk documents in a directory and upload them 
    to S3 with a given bucketname
    """
    for (dirpath, dirnames, filenames) in walk(docfolder):
        documents.extend(filenames)

    for document in documents:
        s3.upload_file(docfolder + "/" + document, bucket, document)
        print(f"Uploaded document = {document}")
    print(documents)
    return documents


def send_docs_to_textract(documents, jobs, bucket, textract):
    """
    Sends All documents to the Textract Async API (used for pdf docs)
    Note the sync API is not implemented.
    """
    for document in documents:
        print(f"Adding {document}")
        response = textract.start_document_text_detection(
            DocumentLocation={"S3Object": {"Bucket": bucket, "Name": document}}
        )
        jobs.append(response["JobId"])
        print(f'Job with ID \n \t {response["JobId"]} \n started')

    return jobs
    # print(jobs)


def isJobComplete(jobId, textract):
    '''
    Checks if the jobs stored in the jobs lists are complete. 
    '''
    time.sleep(5)
    # client = boto3.client("textract")
    response = textract.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while status == "IN_PROGRESS":
        time.sleep(5)
        response = textract.get_document_text_detection(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status


def getJobResults(jobId):
    '''
    Get the resultes of the job. 
    '''

    pages = []

    time.sleep(5)

    # This is poor - will need to modify this. 
    client = boto3.client("textract")
    response = client.get_document_text_detection(JobId=jobId)

    pages.append(response)
    print("Resultset page received: {}".format(len(pages)))
    nextToken = None
    if "NextToken" in response:
        nextToken = response["NextToken"]

    while nextToken:
        time.sleep(5)

        response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page received: {}".format(len(pages)))
        nextToken = None
        if "NextToken" in response:
            nextToken = response["NextToken"]

    return pages
